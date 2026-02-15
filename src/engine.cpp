// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#include "spdk_kv/engine.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>

#include "spdk_kv/recovery.h"

namespace spdk_kv {

Engine::Engine()
        : state_(EngineState::kUninitialized),
          active_file_id_(0),
          next_file_id_(0),
          total_data_bytes_(0),
          total_garbage_bytes_(0),
          compaction_enabled_(true),
          pending_foreground_count_(0),
          allocating_new_file_(false),
          pending_checkpoint_for_alloc_(false),
          pending_blob_ops_(0),
          superblock_blob_(nullptr),
          superblock_blob_id_(SPDK_BLOBID_INVALID),
          next_global_slot_id_(0),
          last_flush_ns_(0) {
    std::memset(&superblock_, 0, sizeof(superblock_));
}

Engine::~Engine() {
    if (state_ == EngineState::kReady) {
        close();
    }
}

KvError Engine::create(const std::string& path, const CreateOpts& opts) {
    if (state_ != EngineState::kUninitialized) {
        return KvError::kInvalidState;
    }

    state_ = EngineState::kOpening;
    path_ = path;
    config_ = opts.config;

    KvError err = initialize_new(opts);
    if (err != KvError::kSuccess) {
        state_ = EngineState::kError;
        return err;
    }

    state_ = EngineState::kReady;
    return KvError::kSuccess;
}

KvError Engine::open(const std::string& path, const OpenOpts& opts) {
    if (state_ != EngineState::kUninitialized) {
        return KvError::kInvalidState;
    }

    state_ = EngineState::kOpening;
    path_ = path;

    KvError err = load_existing(opts);
    if (err != KvError::kSuccess) {
        state_ = EngineState::kError;
        return err;
    }

    if (opts.recover) {
        state_ = EngineState::kRecovering;
        err = recover();
        if (err != KvError::kSuccess) {
            state_ = EngineState::kError;
            return err;
        }
    }

    state_ = EngineState::kReady;
    return KvError::kSuccess;
}

KvError Engine::close() {
    if (state_ != EngineState::kReady) {
        return KvError::kInvalidState;
    }

    state_ = EngineState::kClosing;

    // Flush any pending writes
    if (buffer_manager_) {
        buffer_manager_->submit_current_buffer();

        // Drain all pending buffers
        AppendBuffer* buffers[16];
        size_t count;
        do {
            count = buffer_manager_->get_pending_buffers(buffers, 16);
            for (size_t i = 0; i < count; i++) {
                submit_buffer_io(buffers[i]);
            }
        } while (count > 0);
    }

    // Fail remaining wait_queue_ entries
    while (!wait_queue_.empty()) {
        WaitQueueEntry wqe = wait_queue_.front();
        wait_queue_.pop();
        if (wqe.dma_buffer) {
            DmaAllocator::free(wqe.dma_buffer);
        }
        if (wqe.callback) {
            wqe.callback(wqe.cb_arg, static_cast<int>(KvError::kEngineNotReady));
        }
    }

    // clear buffer pending writes
    buffer_pending_writes_.clear();

    // Update superblock with latest state before persisting
    superblock_.active_file_id = active_file_id_;
    superblock_.total_data_bytes = total_data_bytes_;
    superblock_.total_garbage_bytes = total_garbage_bytes_;
    superblock_.total_entries = mem_index_ ? mem_index_->size() : 0;

    // Persist AllocLog state
    if (alloc_log_manager_) {
        superblock_.alloc_log_head = alloc_log_manager_->get_head();
        superblock_.alloc_log_tail = alloc_log_manager_->get_tail();
        superblock_.alloc_log_sequence = alloc_log_manager_->get_sequence();
    }

    // Update file mappings with current state
    for (const auto& file : files_) {
        update_superblock_file_mapping(file.get());
    }

    // Persist superblock
    if (superblock_blob_) {
        write_superblock();
    }

    // close all open data blobs
    for (auto& file : files_) {
        if (file->blob_opened && file->blob) {
            close_blob_for_file(file.get(), nullptr);
        }
    }

    // Wait for pending blob operations to complete
    while (pending_blob_ops_ > 0) {
        if (io_submitter_) {
            io_submitter_->process_completions(32);
        }
        SpdkEnv::instance().poll();
    }

    // close superblock blob
    if (superblock_blob_) {
        auto& env = SpdkEnv::instance();
        if (env.is_initialized()) {
            struct CloseCtx {
                bool done;
            };
            CloseCtx close_ctx{false};

            env.close_blob(superblock_blob_, [&close_ctx](int) { close_ctx.done = true; });

            while (!close_ctx.done) {
                env.poll();
            }
        }
        superblock_blob_ = nullptr;
    }

    // Clean up resources
    files_.clear();
    mem_index_.reset();
    buffer_manager_.reset();
    io_submitter_.reset();
    checkpoint_manager_.reset();
    compaction_scheduler_.reset();
    alloc_log_manager_.reset();

    state_ = EngineState::kClosed;
    return KvError::kSuccess;
}

KvError Engine::initialize_new(const CreateOpts& opts) {
    (void)opts;  // Suppress unused parameter warning (used for future extension)
    // initialize memory index
    mem_index_ = std::make_unique<MemIndex>(config_.max_entries, config_.index_load_factor);
    if (!mem_index_) {
        return KvError::kInternalError;
    }

    // initialize append buffer manager
    buffer_manager_ = std::make_unique<AppendBufferManager>();
    if (!buffer_manager_->initialize(config_.append_buffer_count, config_.append_buffer_size)) {
        return KvError::kInternalError;
    }

    // initialize checkpoint manager
    checkpoint_manager_ =
            std::make_unique<IncrementalCheckpoint>(mem_index_.get(), mem_index_->capacity());
    checkpoint_manager_->set_append_buffer_manager(buffer_manager_.get());

    // initialize compaction scheduler
    compaction_scheduler_ = std::make_unique<CompactionScheduler>(this);

    // initialize IO submitter
    io_submitter_ = std::make_unique<IoSubmitter>();

    // In SPDK mode, initialize with SPDK resources
    auto& env = SpdkEnv::instance();
    if (env.is_initialized()) {
        io_submitter_->initialize(env.get_controller(), env.get_namespace(), env.get_blob_store(),
                                  config_.io_queue_size);
    } else {
        return KvError::kInternalError;
    }

    // initialize superblock in memory (before allocate_new_file, which updates file_mappings)
    auto now = std::chrono::system_clock::now().time_since_epoch();
    superblock_.magic = kSuperblockMagic;
    superblock_.version = 1;
    superblock_.sequence = 0;
    superblock_.create_time = std::chrono::duration_cast<std::chrono::seconds>(now).count();
    superblock_.last_mount_time = superblock_.create_time;
    superblock_.total_capacity = config_.max_capacity;
    superblock_.data_file_size = config_.data_file_size;
    superblock_.alignment_unit = kPageSize;
    superblock_.file_count = 0;

    // create superblock blob first (needed for AllocLog writes)
    KvError sb_err = create_superblock_blob();
    if (sb_err != KvError::kSuccess) {
        return sb_err;
    }

    // initialize AllocLog manager
    init_alloc_log_manager();

    // create first data file (this calls update_superblock_file_mapping internally,
    // and now also writes AllocLog entry before DataFileHeader)
    FileInfo* file = allocate_new_file();
    if (!file) {
        return KvError::kInternalError;
    }
    active_file_id_ = file->file_id;
    superblock_.active_file_id = active_file_id_;

    // Persist superblock (includes AllocLog state)
    if (alloc_log_manager_) {
        superblock_.alloc_log_head = alloc_log_manager_->get_head();
        superblock_.alloc_log_tail = alloc_log_manager_->get_tail();
        superblock_.alloc_log_sequence = alloc_log_manager_->get_sequence();
    }

    sb_err = write_superblock();
    if (sb_err != KvError::kSuccess) {
        return sb_err;
    }

    // set superblock update callback for checkpoint atomicity
    checkpoint_manager_->set_superblock_update_callback(
            [this](uint32_t checkpoint_seq, const ActiveBufferPos* positions, uint8_t count,
                   std::function<void(int)> on_complete) {
                on_superblock_update(checkpoint_seq, positions, count, std::move(on_complete));
            });

    return KvError::kSuccess;
}

KvError Engine::load_existing(const OpenOpts& opts) {
    // step 1: Load superblock from persistent storage
    KvError err = load_superblock();
    if (err != KvError::kSuccess) {
        return err;
    }

    // step 2: Validate superblock
    if (!superblock_.is_valid()) {
        return KvError::kCorruption;
    }

    // step 3: Load configuration from superblock
    config_.max_capacity = superblock_.total_capacity;
    config_.data_file_size = superblock_.data_file_size;
    // Keep other config values at defaults or load from persistent storage if available

    // step 3.5: initialize AllocLog manager (needs superblock_blob_ from load_superblock)
    init_alloc_log_manager();

    // step 3.6: Load and process AllocLog entries for recovery
    process_alloc_log_recovery();

    // step 4: initialize memory index
    mem_index_ = std::make_unique<MemIndex>(config_.max_entries, config_.index_load_factor);
    if (!mem_index_) {
        return KvError::kInternalError;
    }

    // step 5: initialize append buffer manager
    buffer_manager_ = std::make_unique<AppendBufferManager>();
    if (!buffer_manager_->initialize(config_.append_buffer_count, config_.append_buffer_size)) {
        return KvError::kInternalError;
    }

    // step 6: initialize IO submitter
    io_submitter_ = std::make_unique<IoSubmitter>();
    auto& env = SpdkEnv::instance();
    if (env.is_initialized()) {
        io_submitter_->initialize(env.get_controller(), env.get_namespace(), env.get_blob_store(),
                                  config_.io_queue_size);
    } else {
        return KvError::kInternalError;
    }

    // step 7: Rebuild file info from superblock
    err = rebuild_file_info();
    if (err != KvError::kSuccess) {
        return err;
    }

    // step 7.5: release dangling blobs (allocated but not tracked in file_mappings or AllocLog)
    release_dangling_blobs();

    // step 8: initialize checkpoint manager
    checkpoint_manager_ =
            std::make_unique<IncrementalCheckpoint>(mem_index_.get(), mem_index_->capacity());
    checkpoint_manager_->set_append_buffer_manager(buffer_manager_.get());

    // set superblock update callback for checkpoint atomicity
    checkpoint_manager_->set_superblock_update_callback(
            [this](uint32_t checkpoint_seq, const ActiveBufferPos* positions, uint8_t count,
                   std::function<void(int)> on_complete) {
                on_superblock_update(checkpoint_seq, positions, count, std::move(on_complete));
            });

    // step 9: initialize compaction scheduler
    compaction_scheduler_ = std::make_unique<CompactionScheduler>(this);

    // step 10: Restore statistics from superblock
    total_data_bytes_ = superblock_.total_data_bytes;
    total_garbage_bytes_ = superblock_.total_garbage_bytes;
    active_file_id_ = superblock_.active_file_id;
    next_file_id_ = superblock_.file_count;

    // step 11: Restore global sequence number from checkpoint
    // This will be updated during recovery if needed
    mem_index_->set_global_sequence(superblock_.checkpoint_global_seq);

    // step 12: Perform recovery if requested
    if (opts.recover) {
        err = recover_mem_index();
        if (err != KvError::kSuccess) {
            return err;
        }
    }

    // step 13: Update last mount time in superblock
    auto now = std::chrono::system_clock::now().time_since_epoch();
    superblock_.last_mount_time = std::chrono::duration_cast<std::chrono::seconds>(now).count();

    return KvError::kSuccess;
}

KvError Engine::load_superblock() {
    auto& env = SpdkEnv::instance();
    if (!env.is_initialized() || !env.get_blob_store()) {
        return KvError::kInternalError;
    }

    // step 1: get super blob ID from blobstore
    struct GetSuperCtx {
        spdk_blob_id blob_id;
        int status;
        bool done;
    };
    GetSuperCtx get_ctx{SPDK_BLOBID_INVALID, 0, false};

    spdk_bs_get_super(
            env.get_blob_store(),
            [](void* arg, spdk_blob_id blobid, int bserrno) {
                auto* ctx = static_cast<GetSuperCtx*>(arg);
                ctx->blob_id = blobid;
                ctx->status = bserrno;
                ctx->done = true;
            },
            &get_ctx);

    while (!get_ctx.done) {
        env.poll();
    }

    if (get_ctx.status != 0 || get_ctx.blob_id == SPDK_BLOBID_INVALID) {
        return KvError::kCorruption;
    }

    superblock_blob_id_ = get_ctx.blob_id;

    // step 2: open the super blob
    struct OpenCtx {
        spdk_blob* blob;
        int status;
        bool done;
    };
    OpenCtx open_ctx{nullptr, 0, false};

    spdk_bs_open_blob(
            env.get_blob_store(), superblock_blob_id_,
            [](void* arg, struct spdk_blob* blob, int bserrno) {
                auto* ctx = static_cast<OpenCtx*>(arg);
                ctx->blob = blob;
                ctx->status = bserrno;
                ctx->done = true;
            },
            &open_ctx);

    while (!open_ctx.done) {
        env.poll();
    }

    if (open_ctx.status != 0 || !open_ctx.blob) {
        return KvError::kCorruption;
    }

    superblock_blob_ = open_ctx.blob;

    // step 3: Read primary superblock
    size_t sb_aligned_size = align_up(sizeof(Superblock), kPageSize);
    void* read_buf = DmaAllocator::alloc_zeroed(sb_aligned_size, kPageSize);
    if (!read_buf) {
        return KvError::kInternalError;
    }

    auto* channel = env.get_io_channel();
    if (!channel) {
        DmaAllocator::free(read_buf);
        return KvError::kInternalError;
    }

    uint64_t io_unit_size = spdk_bs_get_io_unit_size(env.get_blob_store());
    uint64_t length_units = sb_aligned_size / io_unit_size;

    struct ReadCtx {
        int status;
        bool done;
    };
    ReadCtx read_ctx{0, false};

    spdk_blob_io_read(
            superblock_blob_, channel, read_buf, kSuperblockPrimaryOffset / io_unit_size,
            length_units,
            [](void* arg, int bserrno) {
                auto* ctx = static_cast<ReadCtx*>(arg);
                ctx->status = bserrno;
                ctx->done = true;
            },
            &read_ctx);

    while (!read_ctx.done) {
        env.poll();
    }

    if (read_ctx.status == 0) {
        // Validate primary superblock
        auto* sb = static_cast<Superblock*>(read_buf);
        if (sb->magic == kSuperblockMagic) {
            uint32_t stored_checksum = sb->checksum;
            uint32_t computed = Crc32::calculate(sb, sizeof(Superblock) - sizeof(uint32_t));
            if (stored_checksum == computed) {
                std::memcpy(&superblock_, sb, sizeof(Superblock));
                DmaAllocator::free(read_buf);
                return KvError::kSuccess;
            }
        }
    }

    // step 4: Primary invalid, try backup superblock at kSuperblockBackupOffset
    ReadCtx backup_ctx{0, false};

    spdk_blob_io_read(
            superblock_blob_, channel, read_buf, kSuperblockBackupOffset / io_unit_size,
            length_units,
            [](void* arg, int bserrno) {
                auto* ctx = static_cast<ReadCtx*>(arg);
                ctx->status = bserrno;
                ctx->done = true;
            },
            &backup_ctx);

    while (!backup_ctx.done) {
        env.poll();
    }

    if (backup_ctx.status == 0) {
        auto* sb = static_cast<Superblock*>(read_buf);
        if (sb->magic == kSuperblockMagic) {
            uint32_t stored_checksum = sb->checksum;
            uint32_t computed = Crc32::calculate(sb, sizeof(Superblock) - sizeof(uint32_t));
            if (stored_checksum == computed) {
                std::memcpy(&superblock_, sb, sizeof(Superblock));
                DmaAllocator::free(read_buf);
                return KvError::kSuccess;
            }
        }
    }

    DmaAllocator::free(read_buf);
    return KvError::kCorruption;
}

KvError Engine::rebuild_file_info() {
    files_.clear();

    auto& env = SpdkEnv::instance();
    if (!env.is_initialized()) {
        return KvError::kInternalError;
    }

    size_t pending_opens = 0;
    bool open_error = false;

    // Rebuild file info from superblock's file_mappings
    for (uint16_t i = 0; i < superblock_.file_count; i++) {
        const FileMapping& mapping = superblock_.file_mappings[i];

        auto file = std::make_unique<FileInfo>();
        file->file_id = mapping.file_id;
        file->blob_id = mapping.blob_id;
        file->state = mapping.state;
        file->size = mapping.size;
        file->write_offset = mapping.write_offset;
        file->blob = nullptr;
        file->blob_opened = false;

        // initialize file metadata for compaction tracking
        auto& meta = file_metadata_[file->file_id];
        meta.file_id = file->file_id;
        meta.state = file->state;
        meta.total_entries = 0;
        meta.valid_entries = 0;
        meta.total_bytes = file->size;
        meta.valid_bytes = 0;

        // Track next_file_id_
        if (file->file_id >= next_file_id_) {
            next_file_id_ = file->file_id + 1;
        }

        FileInfo* ptr = file.get();
        files_.push_back(std::move(file));

        // open the existing blob (without writing header - data already on disk)
        pending_opens++;
        pending_blob_ops_++;
        env.open_blob(ptr->blob_id, [this, ptr, &pending_opens, &open_error](spdk_blob* blob) {
            pending_blob_ops_--;
            pending_opens--;
            if (!blob) {
                open_error = true;
                return;
            }
            ptr->blob = blob;
            ptr->blob_opened = true;
        });
    }

    // Wait for all blob opens to complete
    while (pending_opens > 0) {
        env.poll();
    }

    if (open_error) {
        return KvError::kIoError;
    }

    // If no files exist, create the first one
    if (files_.empty()) {
        FileInfo* new_file = allocate_new_file();
        if (!new_file) {
            return KvError::kInternalError;
        }
        active_file_id_ = new_file->file_id;
    }

    return KvError::kSuccess;
}

KvError Engine::recover_mem_index() {
    // Use IndexLoader to recover MemIndex from checkpoint and data files
    IndexLoader loader(mem_index_.get());

    // set the superblock for the loader
    loader.set_superblock(superblock_);

    // set SPDK resources if available (for real blob-based recovery)
    auto& env = SpdkEnv::instance();
    if (env.is_initialized() && superblock_blob_) {
        // Determine MemIndex blob handles from superblock
        // NOTE: In the current design, MemIndex blobs need to be opened separately.
        // For now, pass nullptr for MemIndex blobs (they would need dedicated blob allocation).
        spdk_blob* mem_index_blob_a = nullptr;
        spdk_blob* mem_index_blob_b = nullptr;

        loader.set_spdk_resources(env.get_blob_store(), env.get_io_channel(), superblock_blob_,
                                mem_index_blob_a, mem_index_blob_b);

        // Pass data blob info for incremental recovery scan
        std::vector<IndexLoader::DataBlobInfo> data_blobs;
        for (const auto& file : files_) {
            if (file->state != FileState::kDeleted && file->blob_opened && file->blob) {
                IndexLoader::DataBlobInfo bi;
                bi.file_id = file->file_id;
                bi.blob = file->blob;
                bi.size = file->size;
                data_blobs.push_back(bi);
            }
        }
        loader.set_data_blobs(data_blobs);
    }

    // start recovery
    KvError recovery_error = KvError::kSuccess;
    loader.start_recovery([&recovery_error](KvError status) { recovery_error = status; });

    // poll until recovery completes
    while (loader.poll()) {
        // Process completions while recovery is in progress
    }

    if (loader.is_success()) {
        // Update global sequence from recovery
        uint32_t recovered_max_seq = loader.get_recovered_max_sequence();
        uint32_t checkpoint_seq = superblock_.checkpoint_global_seq;
        uint32_t new_global_seq = std::max(checkpoint_seq, recovered_max_seq) + 1;
        mem_index_->set_global_sequence(new_global_seq);
        return KvError::kSuccess;
    }

    return recovery_error;
}

KvError Engine::recover() {
    // Recovery is handled by recover_mem_index() called from load_existing()
    return KvError::kSuccess;
}

FileInfo* Engine::allocate_new_file() {
    // Check AllocLog capacity: if near-full, run checkpoint synchronously to reclaim
    if (alloc_log_manager_ && alloc_log_manager_->is_near_full()) {
        bool cp_done = false;
        start_checkpoint([](void* arg, int) { *static_cast<bool*>(arg) = true; }, &cp_done);
        while (!cp_done) {
            SpdkEnv::instance().poll();
        }
    }

    auto file = std::make_unique<FileInfo>();
    file->file_id = next_file_id_++;
    file->blob_id = 0;  // Will be assigned by SPDK
    file->state = FileState::kActive;
    file->size = 0;
    file->write_offset = sizeof(DataFileHeader);  // Skip header
    file->blob = nullptr;
    file->blob_opened = false;

    FileInfo* ptr = file.get();
    files_.push_back(std::move(file));

    // Allocate blob synchronously (poll until allocation + open + header write completes)
    bool alloc_done = false;
    bool alloc_success = false;
    allocate_blob_for_file(ptr, [&alloc_done, &alloc_success](bool success) {
        alloc_success = success;
        alloc_done = true;
    });

    while (!alloc_done) {
        SpdkEnv::instance().poll();
    }

    if (!alloc_success) {
        files_.pop_back();
        next_file_id_--;
        return nullptr;
    }

    // initialize file metadata for compaction tracking
    auto& meta = file_metadata_[ptr->file_id];
    meta.file_id = ptr->file_id;
    meta.state = ptr->state;
    meta.total_entries = 0;
    meta.valid_entries = 0;
    meta.total_bytes = 0;
    meta.valid_bytes = 0;

    // Update superblock file mapping
    update_superblock_file_mapping(ptr);

    return ptr;
}

void Engine::allocate_new_file_async() {
    allocating_new_file_ = true;

    // Check AllocLog capacity: if near-full, checkpoint first, then resume allocation
    if (alloc_log_manager_ && alloc_log_manager_->is_near_full() && !pending_checkpoint_for_alloc_) {
        pending_checkpoint_for_alloc_ = true;
        start_checkpoint(on_checkpoint_for_alloc_complete, this);
        return;
    }

    auto file = std::make_unique<FileInfo>();
    file->file_id = next_file_id_++;
    file->blob_id = 0;
    file->state = FileState::kActive;
    file->size = 0;
    file->write_offset = sizeof(DataFileHeader);
    file->blob = nullptr;
    file->blob_opened = false;

    FileInfo* ptr = file.get();
    files_.push_back(std::move(file));

    allocate_blob_for_file(ptr, [this, ptr](bool success) { on_new_file_allocated(success); });
}

void Engine::on_new_file_allocated(bool success) {
    allocating_new_file_ = false;

    if (!success) {
        // Rollback file allocation
        files_.pop_back();
        next_file_id_--;

        // Fail all pending writes
        for (auto& req : pending_write_queue_) {
            DmaAllocator::free(req.dma_buffer);
            if (req.cb) {
                req.cb(req.cb_arg, static_cast<int>(KvError::kNoSpace));
            }
        }
        pending_write_queue_.clear();
        return;
    }

    // get the newly created file (last in files_)
    FileInfo* new_file = files_.back().get();

    // initialize file metadata for compaction tracking
    auto& meta = file_metadata_[new_file->file_id];
    meta.file_id = new_file->file_id;
    meta.state = new_file->state;
    meta.total_entries = 0;
    meta.valid_entries = 0;
    meta.total_bytes = 0;
    meta.valid_bytes = 0;

    // Update superblock file mapping
    update_superblock_file_mapping(new_file);

    active_file_id_ = new_file->file_id;

    // Process all queued writes
    process_pending_write_queue();
}

void Engine::process_pending_write_queue() {
    FileInfo* file = get_active_file();
    if (!file || !file->blob_opened) {
        // Should not happen, but handle gracefully
        for (auto& req : pending_write_queue_) {
            if (!req.is_segment_write) {
                DmaAllocator::free(req.dma_buffer);
            }
            if (req.cb) {
                req.cb(req.cb_arg, static_cast<int>(KvError::kInternalError));
            }
        }
        pending_write_queue_.clear();
        return;
    }

    // Move queue to local to avoid issues if callbacks re-enter
    auto queue = std::move(pending_write_queue_);
    pending_write_queue_.clear();

    // TODO: 这里file的空间不应该足够
    for (auto& req : queue) {
        submit_queued_write(req, file);
    }
}

void Engine::fill_pending_write_req(PendingWriteRequest& req, uint64_t key, void* dma_buffer,
                                 uint32_t aligned_size, uint32_t sequence, uint16_t page_count,
                                 uint8_t tag, KvCallback cb, void* cb_arg, bool is_delete,
                                 uint64_t old_garbage_size) {
    req.key = key;
    req.dma_buffer = dma_buffer;
    req.is_segment_write = false;
    req.aligned_size = aligned_size;
    req.sequence = sequence;
    req.page_count = page_count;
    req.tag = tag;
    req.cb = cb;
    req.cb_arg = cb_arg;
    req.is_delete = is_delete;
    req.old_garbage_size = old_garbage_size;
}

void Engine::fill_pending_write_req(PendingWriteRequest& req, uint64_t key,
                                 const SegmentBuf& segment_buf, uint32_t aligned_size,
                                 uint32_t sequence, uint16_t page_count, uint8_t tag, KvCallback cb,
                                 void* cb_arg) {
    fill_pending_write_req(req, key, nullptr, aligned_size, sequence, page_count, tag, cb, cb_arg,
                        false, 0);
    req.segment_buf = segment_buf;
    req.is_segment_write = true;
}

void Engine::fill_pending_write_req(PendingWriteRequest& req, const WaitQueueEntry& wqe) {
    fill_pending_write_req(req, wqe.key, wqe.dma_buffer, wqe.aligned_size, wqe.sequence,
                        wqe.page_count, wqe.tag, wqe.callback, wqe.cb_arg, wqe.is_delete,
                        wqe.old_garbage_size);
}

void Engine::submit_queued_write(PendingWriteRequest& req, FileInfo* file) {
    // Assign file-specific fields
    MemIndexEntry entry;
    entry.key = req.key;
    entry.file_id = file->file_id;
    entry.offset_index = static_cast<uint32_t>(file->write_offset / kPageSize);
    entry.page_count = req.page_count;
    entry.deleted = req.is_delete ? 1 : 0;
    entry.sequence = req.sequence;
    entry.tag = req.tag;

    uint64_t write_offset = file->write_offset;

    file->write_offset += req.aligned_size;
    file->size = file->write_offset;
    total_data_bytes_ += req.aligned_size;

    pending_foreground_count_++;

    auto* ctx = queued_write_ctx_pool_.alloc(this, req.key, entry, req.is_delete,
                                             req.old_garbage_size, req.cb, req.cb_arg);

    if (req.is_segment_write) {
        // Zero-copy path: writev the caller's segment buffers directly
        submit_blob_writev(file, write_offset, req.segment_buf, req.aligned_size,
                         [ctx](int status) { ctx->engine->on_queued_write_complete(status, ctx); });
    } else {
        // Legacy path: single DMA buffer
        void* dma_buffer = req.dma_buffer;
        submit_blob_write(file, write_offset, dma_buffer, req.aligned_size,
                        [dma_buffer, ctx](int status) {
                            DmaAllocator::free(dma_buffer);
                            ctx->engine->on_queued_write_complete(status, ctx);
                        });
    }
}

FileInfo* Engine::get_active_file() { return get_file(active_file_id_); }

FileInfo* Engine::get_file(uint16_t file_id) {
    for (auto& file : files_) {
        if (file->file_id == file_id) {
            return file.get();
        }
    }
    return nullptr;
}

void Engine::build_entry_inplace(void* slot, uint64_t key, const void* value, uint32_t len,
                               uint32_t seq, bool is_tombstone) {
    char* ptr = static_cast<char*>(slot);

    // Header (16 bytes)
    auto* header = reinterpret_cast<EntryHeader*>(ptr);
    header->magic = kEntryMagic;
    header->version = 1;
    header->flags = is_tombstone ? kFlagDeleted : 0;
    header->reserved = 0;
    header->sequence = seq;
    header->padding = 0;
    ptr += sizeof(EntryHeader);

    // Key (8 bytes)
    *reinterpret_cast<uint64_t*>(ptr) = key;
    ptr += sizeof(uint64_t);

    // Value Length (4 bytes)
    *reinterpret_cast<uint32_t*>(ptr) = len;
    ptr += sizeof(uint32_t);

    // Value
    if (value && len > 0) {
        std::memcpy(ptr, value, len);
        ptr += len;
    }

    // calculate padding
    size_t used = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) + len;
    size_t aligned = align_up(used + sizeof(uint32_t), kPageSize);
    size_t padding = aligned - used - sizeof(uint32_t);
    if (padding > 0) {
        std::memset(ptr, 0, padding);
        ptr += padding;
    }

    // Checksum
    size_t data_size = ptr - static_cast<char*>(slot);
    *reinterpret_cast<uint32_t*>(ptr) = Crc32::calculate(slot, data_size);
}

KvError Engine::put(uint64_t key, const void* value, uint32_t value_len) {
    if (state_ != EngineState::kReady) {
        return KvError::kEngineNotReady;
    }

    if (value_len == 0) {
        return KvError::kInvalidArgument;
    }

    if (!value) {
        return KvError::kInvalidArgument;
    }

    // calculate entry size
    size_t header_size = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t);
    size_t entry_size = header_size + value_len + sizeof(uint32_t);  // +checksum
    size_t aligned_size = align_up(entry_size, kPageSize);

    // get active file
    FileInfo* file = get_active_file();
    if (!file || !file->blob_opened || file->write_offset + aligned_size > config_.data_file_size) {
        // Seal current file and create new one
        if (file) {
            file->state = FileState::kSealed;
        }
        file = allocate_new_file();
        if (!file) {
            return KvError::kNoSpace;
        }
        active_file_id_ = file->file_id;
    }

    // Allocate DMA buffer and build entry
    void* dma_buffer = DmaAllocator::alloc_zeroed(aligned_size, kPageSize);
    if (!dma_buffer) {
        return KvError::kInternalError;
    }

    // Allocate sequence number
    uint32_t seq = mem_index_->allocate_sequence();

    // Build entry in DMA buffer
    build_entry_inplace(dma_buffer, key, value, value_len, seq);

    // write to blob synchronously (poll until complete)
    uint64_t write_offset = file->write_offset;
    bool write_done = false;
    int write_status = 0;

    submit_blob_write(file, write_offset, dma_buffer, static_cast<uint32_t>(aligned_size),
                    [&write_done, &write_status](int status) {
                        write_status = status;
                        write_done = true;
                    });

    while (!write_done) {
        SpdkEnv::instance().poll();
    }

    DmaAllocator::free(dma_buffer);

    if (write_status != 0) {
        return KvError::kIoError;
    }

    // Update index
    MemIndexEntry entry;
    entry.key = key;
    entry.file_id = file->file_id;
    entry.offset_index = static_cast<uint32_t>(write_offset / kPageSize);
    entry.page_count = static_cast<uint16_t>(aligned_size / kPageSize);
    entry.deleted = 0;
    entry.sequence = seq;

    uint64_t hash;
    HashUtil::compute_hash(key, &hash, &entry.tag);

    // Check for existing entry (for garbage tracking)
    MemIndexEntry* existing = mem_index_->find(key);
    if (existing) {
        // Mark old data as garbage
        uint64_t old_size = existing->page_count * kPageSize;
        total_garbage_bytes_ += old_size;
    }

    mem_index_->upsert(key, entry);

    // Update file offset
    file->write_offset += aligned_size;
    file->size = file->write_offset;
    total_data_bytes_ += aligned_size;

    return KvError::kSuccess;
}

KvError Engine::get(uint64_t key, void* value_buf, uint32_t buf_len, uint32_t* actual_len) {
    if (state_ != EngineState::kReady) {
        return KvError::kEngineNotReady;
    }

    if (!value_buf || buf_len == 0) {
        return KvError::kInvalidArgument;
    }

    // find in index
    MemIndexEntry* entry = mem_index_->find(key);
    if (!entry) {
        return KvError::kKeyNotFound;
    }

    if (entry->is_deleted()) {
        return KvError::kKeyNotFound;
    }

    // get file
    FileInfo* file = get_file(entry->file_id);
    if (!file || !file->is_readable() || !file->blob_opened) {
        return KvError::kIoError;
    }

    // calculate read size (read the full entry)
    uint32_t read_pages = entry->page_count;
    uint32_t read_size = read_pages * kPageSize;

    // Allocate DMA buffer for reading
    void* dma_buffer = DmaAllocator::alloc(read_size, kPageSize);
    if (!dma_buffer) {
        return KvError::kInternalError;
    }

    uint64_t offset = entry->offset_index * kPageSize;

    // Read from blob synchronously (poll until complete)
    bool read_done = false;
    int read_status = 0;

    submit_blob_read(file, offset, dma_buffer, read_size, [&read_done, &read_status](int status) {
        read_status = status;
        read_done = true;
    });

    while (!read_done) {
        SpdkEnv::instance().poll();
    }

    if (read_status != 0) {
        DmaAllocator::free(dma_buffer);
        return KvError::kIoError;
    }

    // Parse entry header
    auto* header = static_cast<EntryHeader*>(dma_buffer);
    if (!header->is_valid()) {
        DmaAllocator::free(dma_buffer);
        return KvError::kCorruption;
    }

    // get value length
    uint32_t value_len = *reinterpret_cast<uint32_t*>(static_cast<char*>(dma_buffer) +
                                                      sizeof(EntryHeader) + sizeof(uint64_t));

    if (actual_len) {
        *actual_len = value_len;
    }

    if (value_len > buf_len) {
        DmaAllocator::free(dma_buffer);
        return KvError::kValueTooLarge;
    }

    // Copy value
    void* value_ptr = static_cast<char*>(dma_buffer) + sizeof(EntryHeader) + sizeof(uint64_t) +
                      sizeof(uint32_t);
    std::memcpy(value_buf, value_ptr, value_len);

    DmaAllocator::free(dma_buffer);
    return KvError::kSuccess;
}

KvError Engine::del(uint64_t key) {
    if (state_ != EngineState::kReady) {
        return KvError::kEngineNotReady;
    }

    // Check if key exists
    MemIndexEntry* existing = mem_index_->find(key);
    if (!existing) {
        return KvError::kKeyNotFound;
    }

    // calculate tombstone size
    size_t header_size = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t);
    size_t entry_size = header_size + sizeof(uint32_t);  // +checksum
    size_t aligned_size = align_up(entry_size, kPageSize);

    // Track garbage from the old entry
    uint64_t old_size = existing->page_count * kPageSize;

    // get active file
    FileInfo* file = get_active_file();
    if (!file || !file->blob_opened || file->write_offset + aligned_size > config_.data_file_size) {
        if (file) {
            file->state = FileState::kSealed;
        }
        file = allocate_new_file();
        if (!file) {
            return KvError::kNoSpace;
        }
        active_file_id_ = file->file_id;
    }

    // Allocate DMA buffer and build tombstone entry
    void* dma_buffer = DmaAllocator::alloc_zeroed(aligned_size, kPageSize);
    if (!dma_buffer) {
        return KvError::kInternalError;
    }

    // Allocate sequence number
    uint32_t seq = mem_index_->allocate_sequence();

    // Build tombstone entry in DMA buffer
    build_entry_inplace(dma_buffer, key, nullptr, 0, seq, true);

    // write to blob synchronously (poll until complete)
    uint64_t write_offset = file->write_offset;
    bool write_done = false;
    int write_status = 0;

    submit_blob_write(file, write_offset, dma_buffer, static_cast<uint32_t>(aligned_size),
                    [&write_done, &write_status](int status) {
                        write_status = status;
                        write_done = true;
                    });

    while (!write_done) {
        SpdkEnv::instance().poll();
    }

    DmaAllocator::free(dma_buffer);

    if (write_status != 0) {
        return KvError::kIoError;
    }

    // Update index
    mem_index_->remove(key);

    // Track garbage
    total_garbage_bytes_ += old_size;

    // Update file offset
    file->write_offset += aligned_size;
    file->size = file->write_offset;
    total_data_bytes_ += aligned_size;

    return KvError::kSuccess;
}

void Engine::put_async(uint64_t key, SegmentBuf input_buf, KvCallback cb, void* cb_arg) {
    if (state_ != EngineState::kReady) {
        if (cb) {
            cb(cb_arg, static_cast<int>(KvError::kEngineNotReady));
        }
        return;
    }

    if (input_buf.cnt_ == 0) {
        if (cb) {
            cb(cb_arg, static_cast<int>(KvError::kInvalidArgument));
        }
        return;
    }

    // First segment must be at least one page to hold user data [0,256) + metadata [256,512)
    if (input_buf.buffers_[0].iov_len < kPageSize) {
        if (cb) {
            cb(cb_arg, static_cast<int>(KvError::kInvalidArgument));
        }
        return;
    }

    // calculate total size (each buffer is 4KB-aligned per caller contract)
    uint32_t total_size = 0;
    for (size_t i = 0; i < input_buf.cnt_; i++) {
        total_size += static_cast<uint32_t>(input_buf.buffers_[i].iov_len);
    }

    uint16_t page_count = static_cast<uint16_t>(total_size / kPageSize);

    // Compute CRC of value data (user data portions, excluding metadata area [256,512))
    auto* first_base = static_cast<const char*>(input_buf.buffers_[0].iov_base);
    // CRC over [0, 256) of first segment
    uint32_t crc = Crc32::calculate(first_base, kSegmentMetaOffset);
    // CRC over [512, end) of first segment
    if (input_buf.buffers_[0].iov_len > kSegmentDataOffset) {
        crc = Crc32::combine(crc, first_base + kSegmentDataOffset,
                             input_buf.buffers_[0].iov_len - kSegmentDataOffset);
    }
    // CRC over remaining segments
    for (size_t i = 1; i < input_buf.cnt_; i++) {
        crc = Crc32::combine(crc, input_buf.buffers_[i].iov_base, input_buf.buffers_[i].iov_len);
    }

    // Build entry metadata at offset 256 in the first segment (zero-copy: no DMA alloc)
    uint32_t seq = mem_index_->allocate_sequence();
    char* meta_ptr = static_cast<char*>(input_buf.buffers_[0].iov_base) + kSegmentMetaOffset;

    auto* header = reinterpret_cast<EntryHeader*>(meta_ptr);
    header->magic = kEntryMagic;
    header->version = 1;
    header->flags = kFlagSegmentBuf;
    header->reserved = 0;
    header->sequence = seq;
    header->padding = 0;
    meta_ptr += sizeof(EntryHeader);

    // Key (8 bytes)
    *reinterpret_cast<uint64_t*>(meta_ptr) = key;
    meta_ptr += sizeof(uint64_t);

    // Total record size (4 bytes)
    *reinterpret_cast<uint32_t*>(meta_ptr) = total_size;
    meta_ptr += sizeof(uint32_t);

    // CRC of value data (4 bytes)
    *reinterpret_cast<uint32_t*>(meta_ptr) = crc;

    // Compute hash tag for mem index
    uint8_t tag;
    uint64_t hash;
    HashUtil::compute_hash(key, &hash, &tag);

    // get active file
    FileInfo* file = get_active_file();
    bool need_new_file =
            !file || !file->blob_opened || file->write_offset + total_size > config_.data_file_size;

    // If a new file is needed or one is being allocated, queue the write
    if (need_new_file || allocating_new_file_) {
        PendingWriteRequest req{};
        fill_pending_write_req(req, key, input_buf, total_size, seq, page_count, tag, cb, cb_arg);
        pending_write_queue_.push_back(req);

        if (!allocating_new_file_) {
            if (file) {
                file->state = FileState::kSealed;
            }
            allocate_new_file_async();
        }
        return;
    }

    // Normal path: active file is available — submit writev directly
    MemIndexEntry entry;
    entry.key = key;
    entry.file_id = file->file_id;
    entry.offset_index = static_cast<uint32_t>(file->write_offset / kPageSize);
    entry.page_count = page_count;
    entry.deleted = 0;
    entry.sequence = seq;
    entry.tag = tag;

    uint64_t write_offset = file->write_offset;

    file->write_offset += total_size;
    file->size = file->write_offset;
    total_data_bytes_ += total_size;

    pending_foreground_count_++;

    auto* ctx = put_async_ctx_pool_.alloc(this, key, entry, cb, cb_arg);

    submit_blob_writev(file, write_offset, input_buf, total_size,
                     [ctx](int status) { ctx->engine->on_put_async_write_complete(status, ctx); });
}

void Engine::get_async(uint64_t key, SegmentBuf* output, KvGetCallback cb, void* cb_arg) {
    if (state_ != EngineState::kReady) {
        if (cb) {
            cb(cb_arg, static_cast<int>(KvError::kEngineNotReady), 0);
        }
        return;
    }

    if (!output || output->cnt_ == 0) {
        if (cb) {
            cb(cb_arg, static_cast<int>(KvError::kInvalidArgument), 0);
        }
        return;
    }

    // find in index
    MemIndexEntry* entry = mem_index_->find(key);
    if (!entry) {
        if (cb) {
            cb(cb_arg, static_cast<int>(KvError::kKeyNotFound), 0);
        }
        return;
    }

    if (entry->is_deleted()) {
        if (cb) {
            cb(cb_arg, static_cast<int>(KvError::kKeyNotFound), 0);
        }
        return;
    }

    // get file
    FileInfo* file = get_file(entry->file_id);
    if (!file || !file->is_readable() || !file->blob_opened) {
        if (cb) {
            cb(cb_arg, static_cast<int>(KvError::kIoError), 0);
        }
        return;
    }

    // calculate read size (read the full entry including 512-byte header)
    uint32_t read_pages = entry->page_count;
    uint32_t read_size = read_pages * kPageSize;

    // calculate total buffer size provided by user
    // Each segment is 4KB aligned (address), but iov_len can be > 4KB
    uint64_t total_buf_size = 0;
    for (size_t i = 0; i < output->cnt_; i++) {
        total_buf_size += output->buffers_[i].iov_len;
    }

    // Check if user-provided buffer is large enough
    if (total_buf_size < read_size) {
        if (cb) {
            cb(cb_arg, static_cast<int>(KvError::kInvalidArgument), read_size);
        }
        return;
    }

    uint64_t offset = entry->offset_index * kPageSize;

    pending_foreground_count_++;

    // Submit async blob read directly to user-provided buffer (zero-copy)
    auto* read_ctx = get_read_ctx_pool_.alloc(this, output, read_size, cb, cb_arg);

    if (output->cnt_ == 1) {
        // Single contiguous buffer: use standard read
        submit_blob_read(file, offset, output->buffers_[0].iov_base, read_size,
                       [read_ctx](int status) {
                           read_ctx->engine->handle_get_read_completion(status, read_ctx);
                       });
    } else {
        // Multiple buffers: use vectored read
        submit_blob_readv(file, offset, *output, read_size, [read_ctx](int status) {
            read_ctx->engine->handle_get_read_completion(status, read_ctx);
        });
    }
}

void Engine::handle_get_read_completion(int status, GetReadCompletionCtx* ctx) {
    pending_foreground_count_--;

    if (status != 0) {
        if (ctx->cb) {
            ctx->cb(ctx->cb_arg, static_cast<int>(KvError::kIoError), 0);
        }
        get_read_ctx_pool_.free(ctx);
        return;
    }

    // data layout in the first segment (data already in user-provided buffer):
    // [0, 256):     Reserved for user (engine doesn't write here)
    // [256, 512):   Entry metadata (EntryHeader + key + value_len + crc)
    // [512, ...):   Actual value data (user will skip first 512 bytes)

    // Parse entry header at offset 256 (kSegmentMetaOffset) from the first buffer
    auto* header = reinterpret_cast<EntryHeader*>(
            static_cast<char*>(ctx->output->buffers_[0].iov_base) + kSegmentMetaOffset);
    if (!header->is_valid()) {
        if (ctx->cb) {
            ctx->cb(ctx->cb_arg, static_cast<int>(KvError::kCorruption), 0);
        }
        get_read_ctx_pool_.free(ctx);
        return;
    }

    // get value length from metadata area
    // Layout at offset 256: EntryHeader(16) + key(8) + value_len(4)
    uint32_t value_len = *reinterpret_cast<uint32_t*>(
            static_cast<char*>(ctx->output->buffers_[0].iov_base) + kSegmentMetaOffset +
            sizeof(EntryHeader) + sizeof(uint64_t));

    // data is already in user-provided buffer (zero-copy)
    // User is responsible for their own buffer lifecycle

    if (ctx->cb) {
        // Return actual value length (excluding the 512-byte header)
        ctx->cb(ctx->cb_arg, 0, value_len);
    }
    get_read_ctx_pool_.free(ctx);
}

void Engine::delete_async(uint64_t key, KvCallback cb, void* cb_arg) {
    if (state_ != EngineState::kReady) {
        if (cb) {
            cb(cb_arg, static_cast<int>(KvError::kEngineNotReady));
        }
        return;
    }

    // Check if key exists
    MemIndexEntry* existing = mem_index_->find(key);
    if (!existing) {
        if (cb) {
            cb(cb_arg, static_cast<int>(KvError::kKeyNotFound));
        }
        return;
    }

    // calculate tombstone size
    size_t header_size = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t);
    size_t entry_size = header_size + sizeof(uint32_t);  // +checksum
    size_t aligned_size = align_up(entry_size, kPageSize);

    // Track garbage from the old entry
    uint64_t old_size = existing->page_count * kPageSize;

    // get active file
    FileInfo* file = get_active_file();
    bool need_new_file = !file || !file->blob_opened ||
                         file->write_offset + aligned_size > config_.data_file_size;

    // If a new file is needed or one is being allocated, queue the write
    if (need_new_file || allocating_new_file_) {
        void* dma_buffer = DmaAllocator::alloc_zeroed(aligned_size, kPageSize);
        if (!dma_buffer) {
            if (cb) {
                cb(cb_arg, static_cast<int>(KvError::kInternalError));
            }
            return;
        }

        uint32_t seq = mem_index_->allocate_sequence();
        build_entry_inplace(dma_buffer, key, nullptr, 0, seq, true);

        uint8_t tag;
        uint64_t hash;
        HashUtil::compute_hash(key, &hash, &tag);

        PendingWriteRequest req{};
        fill_pending_write_req(req, key, dma_buffer, static_cast<uint32_t>(aligned_size), seq,
                            static_cast<uint16_t>(aligned_size / kPageSize), tag, cb, cb_arg, true,
                            old_size);
        pending_write_queue_.push_back(req);

        if (!allocating_new_file_) {
            if (file) {
                file->state = FileState::kSealed;
            }
            allocate_new_file_async();
        }
        return;
    }

    // Normal path: active file is available
    void* dma_buffer = DmaAllocator::alloc_zeroed(aligned_size, kPageSize);
    if (!dma_buffer) {
        if (cb) {
            cb(cb_arg, static_cast<int>(KvError::kInternalError));
        }
        return;
    }

    uint32_t seq = mem_index_->allocate_sequence();
    build_entry_inplace(dma_buffer, key, nullptr, 0, seq, true);

    uint64_t write_offset = file->write_offset;

    file->write_offset += aligned_size;
    file->size = file->write_offset;
    total_data_bytes_ += aligned_size;

    pending_foreground_count_++;

    submit_blob_write(file, write_offset, dma_buffer, static_cast<uint32_t>(aligned_size),
                    [this, key, dma_buffer, old_size, cb, cb_arg](int status) {
                        DmaAllocator::free(dma_buffer);
                        pending_foreground_count_--;

                        if (status == 0) {
                            mem_index_->remove(key);
                            total_garbage_bytes_ += old_size;
                        }

                        if (cb) {
                            cb(cb_arg, status == 0 ? 0 : static_cast<int>(KvError::kIoError));
                        }
                    });
    return;
}

void Engine::poll() {
    // 1. Process IO completions (high priority)
    if (io_submitter_) {
        io_submitter_->process_completions(32);
    }

    // 2. Execute deferred callback tasks (RDMA send, index updates, user callbacks)
    task_queue_.process_tasks(64);

    // 3. Process append buffer resets
    if (buffer_manager_) {
        buffer_manager_->check_pending_resets();
    }

    // 3. FlushTrigger check: immediate or timeout-based flush
    if (buffer_manager_) {
        size_t pending_buffers = buffer_manager_->pending_count();
        size_t pending_entries = buffer_manager_->current_entry_count();

        auto now = std::chrono::steady_clock::now();
        uint64_t current_ns =
                std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch())
                        .count();

        if (flush_trigger_.should_flush_immediately(pending_buffers, pending_entries) ||
            flush_trigger_.should_flush_on_timeout(last_flush_ns_, current_ns)) {
            buffer_manager_->submit_current_buffer();
            last_flush_ns_ = current_ns;
        }
    }

    // 4. Batch submit pending buffers
    if (buffer_manager_) {
        AppendBuffer* buffers[16];
        size_t count = buffer_manager_->get_pending_buffers(buffers, 16);
        for (size_t i = 0; i < count; i++) {
            submit_buffer_io(buffers[i]);
        }
    }

    // 5. Process resume notifications (backpressure transition)
    if (buffer_manager_) {
        buffer_manager_->process_resume_notifications();
    }

    // 6. Process wait queue if not backpressured
    if (buffer_manager_ && !buffer_manager_->is_backpressure_active() && !wait_queue_.empty()) {
        process_wait_queue();
    }

    // 7. Check checkpoint trigger
    check_checkpoint_trigger();

    // 8. poll checkpoint progress
    if (checkpoint_manager_ && checkpoint_manager_->is_in_progress()) {
        checkpoint_manager_->poll();
    }

    // 9. Process compaction (low priority)
    if (compaction_enabled_ && compaction_scheduler_) {
        compaction_scheduler_->set_pending_foreground_count(get_pending_foreground_count());
        compaction_scheduler_->poll();
    }
}

void Engine::start_checkpoint(KvCallback cb, void* cb_arg) {
    if (!checkpoint_manager_) {
        if (cb) {
            cb(cb_arg, -1);
        }
        return;
    }

    // set global sequence (will be snapshotted atomically in start_checkpoint)
    checkpoint_manager_->set_global_sequence(mem_index_->get_global_sequence());

    // set active buffer positions for snapshot
    // These represent the current write positions in active data files.
    // During recovery, scanning starts from these positions.
    ActiveBufferPos positions[kMaxBufferCount];
    uint8_t pos_count = 0;

    FileInfo* active_file = get_active_file();
    if (active_file) {
        positions[0].file_id = active_file->file_id;
        positions[0].page_index = active_file->write_offset / kPageSize;
        pos_count = 1;
    }
    checkpoint_manager_->set_active_buffer_positions(positions, pos_count);

    checkpoint_manager_->start_checkpoint([cb, cb_arg](int status) {
        if (cb) {
            cb(cb_arg, status);
        }
    });
}

bool Engine::is_checkpoint_in_progress() const {
    return checkpoint_manager_ && checkpoint_manager_->is_in_progress();
}

void Engine::check_checkpoint_trigger() {
    if (!checkpoint_manager_ || is_checkpoint_in_progress()) {
        return;
    }

    // get current time
    auto now = std::chrono::steady_clock::now();
    uint64_t current_time_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();

    if (checkpoint_trigger_.should_checkpoint(checkpoint_manager_->dirty_segment_count(),
                                             current_time_ns)) {
        start_checkpoint(nullptr, nullptr);
        checkpoint_trigger_.reset(current_time_ns);
    }
}

void Engine::schedule_compaction(uint16_t file_id) {
    if (!compaction_scheduler_) {
        return;
    }

    compaction_scheduler_->schedule_compaction(file_id);
}

size_t Engine::get_garbage_ratio() const {
    if (total_data_bytes_ == 0) {
        return 0;
    }
    return (total_garbage_bytes_ * 100) / total_data_bytes_;
}

FileMetadata* Engine::get_file_metadata(uint16_t file_id) {
    auto it = file_metadata_.find(file_id);
    return (it != file_metadata_.end()) ? &it->second : nullptr;
}

const std::unordered_map<uint16_t, FileMetadata>& Engine::get_file_metadata_map() const {
    return file_metadata_;
}

void Engine::compaction_remove_file(uint16_t file_id, std::function<void(bool)> callback) {
    FileInfo* file = get_file(file_id);
    if (!file) {
        if (callback) {
            callback(false);
        }
        return;
    }

    // close the blob first, then delete it
    close_blob_for_file(file, [this, file_id, file, callback](bool close_ok) {
        on_compaction_blob_closed(close_ok, file_id, file, callback);
    });
}

uint64_t Engine::get_entry_count() const { return mem_index_ ? mem_index_->size() : 0; }

uint64_t Engine::get_total_data_bytes() const { return total_data_bytes_; }

double Engine::get_index_load_factor() const { return mem_index_ ? mem_index_->load_factor() : 0.0; }

void Engine::allocate_blob_for_file(FileInfo* file, std::function<void(bool success)> callback) {
    if (!file) {
        if (callback) {
            callback(false);
        }
        return;
    }

    auto& env = SpdkEnv::instance();
    if (!env.is_initialized()) {
        if (callback) {
            callback(false);
        }
        return;
    }

    pending_blob_ops_++;

    // Allocate blob with the configured data file size
    env.allocate_blob(config_.data_file_size, [this, file, callback](uint64_t blob_id) {
        pending_blob_ops_--;
        on_blob_allocated(blob_id, file, callback);
    });
}

void Engine::open_blob_for_file(FileInfo* file, std::function<void(bool success)> callback) {
    if (!file) {
        if (callback) {
            callback(false);
        }
        return;
    }

    auto& env = SpdkEnv::instance();
    if (!env.is_initialized()) {
        if (callback) {
            callback(false);
        }
        return;
    }

    pending_blob_ops_++;

    // Use named completion handler instead of lambda
    auto* open_ctx = open_blob_ctx_pool_.alloc(this, file, std::move(callback));
    env.open_blob(file->blob_id, [open_ctx](spdk_blob* blob) {
        open_ctx->engine->handle_blob_opened_for_file(blob, open_ctx);
    });
}

void Engine::handle_blob_opened_for_file(spdk_blob* blob, OpenBlobForFileCtx* ctx) {
    pending_blob_ops_--;

    if (!blob) {
        if (ctx->callback) {
            ctx->callback(false);
        }
        open_blob_ctx_pool_.free(ctx);
        return;
    }

    ctx->file->blob = blob;
    ctx->file->blob_opened = true;

    // write file header to blob
    DataFileHeader* header =
            static_cast<DataFileHeader*>(DmaAllocator::alloc_zeroed(kPageSize, kPageSize));
    if (!header) {
        if (ctx->callback) {
            ctx->callback(false);
        }
        open_blob_ctx_pool_.free(ctx);
        return;
    }

    header->magic = kDataFileHeaderMagic;
    header->version = 1;
    header->file_id = ctx->file->file_id;
    header->state = FileState::kActive;
    header->create_time = std::chrono::duration_cast<std::chrono::seconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count();
    header->checksum = Crc32::calculate(header, sizeof(*header) - sizeof(header->checksum));

    // write header to blob
    FileInfo* file = ctx->file;
    auto callback = std::move(ctx->callback);
    open_blob_ctx_pool_.free(ctx);

    submit_blob_write(file, 0, header, kPageSize, [header, callback](int status) {
        DmaAllocator::free(header);
        if (callback) {
            callback(status == 0);
        }
    });
}

void Engine::close_blob_for_file(FileInfo* file, std::function<void(bool success)> callback) {
    if (!file || !file->blob_opened || !file->blob) {
        if (callback) {
            callback(true);  // Not an error if no blob to close
        }
        return;
    }

    auto& env = SpdkEnv::instance();
    if (!env.is_initialized()) {
        if (callback) {
            callback(false);
        }
        return;
    }

    pending_blob_ops_++;

    env.close_blob(file->blob, [this, file, callback](int status) {
        pending_blob_ops_--;
        file->blob = nullptr;
        file->blob_opened = false;
        if (callback) {
            callback(status == 0);
        }
    });
}

void Engine::submit_blob_write(FileInfo* file, uint64_t offset, void* data, uint32_t length,
                             std::function<void(int status)> callback) {
    if (!file || !file->blob || !file->blob_opened) {
        if (callback) {
            callback(-1);
        }
        return;
    }

    auto& env = SpdkEnv::instance();
    if (!env.is_initialized() || !env.get_blob_store()) {
        if (callback) {
            callback(-1);
        }
        return;
    }

    auto* channel = io_submitter_->get_channel();
    if (!channel) {
        if (callback) {
            callback(-1);
        }
        return;
    }

    // calculate offset and length in io_unit_size
    uint64_t io_unit_size = spdk_bs_get_io_unit_size(env.get_blob_store());
    uint64_t offset_units = offset / io_unit_size;
    uint64_t length_units = length / io_unit_size;

    // create completion context from pool
    auto* ctx = blob_write_ctx_pool_.alloc(std::move(callback), this);

    spdk_blob_io_write(
            file->blob, channel, data, offset_units, length_units,
            [](void* arg, int bserrno) {
                auto* ctx = static_cast<BlobWriteCtx*>(arg);
                auto cb = std::move(ctx->callback);
                ctx->engine->blob_write_ctx_pool_.free(ctx);
                if (cb) {
                    cb(bserrno);
                }
            },
            ctx);
}

void Engine::submit_blob_writev(FileInfo* file, uint64_t offset, const SegmentBuf& buf,
                              uint32_t total_length, std::function<void(int status)> callback) {
    if (!file || !file->blob || !file->blob_opened) {
        if (callback) {
            callback(-1);
        }
        return;
    }

    auto& env = SpdkEnv::instance();
    if (!env.is_initialized() || !env.get_blob_store()) {
        if (callback) {
            callback(-1);
        }
        return;
    }

    auto* channel = io_submitter_->get_channel();
    if (!channel) {
        if (callback) {
            callback(-1);
        }
        return;
    }

    uint64_t io_unit_size = spdk_bs_get_io_unit_size(env.get_blob_store());
    uint64_t offset_units = offset / io_unit_size;
    uint64_t length_units = total_length / io_unit_size;

    // Completion context holds the callback and a copy of the iovec array
    auto* ctx = blob_writev_ctx_pool_.alloc();
    ctx->callback = std::move(callback);
    ctx->engine = this;
    for (size_t i = 0; i < buf.cnt_; i++) {
        ctx->iovs[i] = buf.buffers_[i];
    }

    spdk_blob_io_writev(
            file->blob, channel, ctx->iovs, static_cast<int>(buf.cnt_), offset_units, length_units,
            [](void* arg, int bserrno) {
                auto* ctx = static_cast<BlobWritevCtx*>(arg);
                auto cb = std::move(ctx->callback);
                ctx->engine->blob_writev_ctx_pool_.free(ctx);
                if (cb) {
                    cb(bserrno);
                }
            },
            ctx);
}

void Engine::submit_blob_read(FileInfo* file, uint64_t offset, void* buffer, uint32_t length,
                            std::function<void(int status)> callback) {
    if (!file || !file->blob || !file->blob_opened) {
        if (callback) {
            callback(-1);
        }
        return;
    }

    auto& env = SpdkEnv::instance();
    if (!env.is_initialized() || !env.get_blob_store()) {
        if (callback) {
            callback(-1);
        }
        return;
    }

    auto* channel = io_submitter_->get_channel();
    if (!channel) {
        if (callback) {
            callback(-1);
        }
        return;
    }

    // calculate offset and length in io_unit_size
    uint64_t io_unit_size = spdk_bs_get_io_unit_size(env.get_blob_store());
    uint64_t offset_units = offset / io_unit_size;
    uint64_t length_units = length / io_unit_size;

    // create completion context from pool
    auto* ctx = blob_read_ctx_pool_.alloc(std::move(callback), this);

    spdk_blob_io_read(
            file->blob, channel, buffer, offset_units, length_units,
            [](void* arg, int bserrno) {
                auto* ctx = static_cast<BlobReadCtx*>(arg);
                auto cb = std::move(ctx->callback);
                ctx->engine->blob_read_ctx_pool_.free(ctx);
                if (cb) {
                    cb(bserrno);
                }
            },
            ctx);
}

void Engine::submit_blob_readv(FileInfo* file, uint64_t offset, const SegmentBuf& buf,
                             uint32_t total_length, std::function<void(int status)> callback) {
    if (!file || !file->blob || !file->blob_opened) {
        if (callback) {
            callback(-1);
        }
        return;
    }

    auto& env = SpdkEnv::instance();
    if (!env.is_initialized() || !env.get_blob_store()) {
        if (callback) {
            callback(-1);
        }
        return;
    }

    auto* channel = io_submitter_->get_channel();
    if (!channel) {
        if (callback) {
            callback(-1);
        }
        return;
    }

    uint64_t io_unit_size = spdk_bs_get_io_unit_size(env.get_blob_store());
    uint64_t offset_units = offset / io_unit_size;
    uint64_t length_units = total_length / io_unit_size;

    // Completion context holds the callback and a copy of the iovec array
    auto* ctx = blob_readv_ctx_pool_.alloc();
    ctx->callback = std::move(callback);
    ctx->engine = this;
    for (size_t i = 0; i < buf.cnt_; i++) {
        ctx->iovs[i] = buf.buffers_[i];
    }

    spdk_blob_io_readv(
            file->blob, channel, ctx->iovs, static_cast<int>(buf.cnt_), offset_units, length_units,
            [](void* arg, int bserrno) {
                auto* ctx = static_cast<BlobReadvCtx*>(arg);
                auto cb = std::move(ctx->callback);
                ctx->engine->blob_readv_ctx_pool_.free(ctx);
                if (cb) {
                    cb(bserrno);
                }
            },
            ctx);
}

spdk_blob* Engine::get_blob_for_file(uint16_t file_id) {
    FileInfo* file = get_file(file_id);
    if (!file || !file->blob_opened) {
        return nullptr;
    }
    return file->blob;
}

KvError Engine::create_superblock_blob() {
    auto& env = SpdkEnv::instance();
    if (!env.is_initialized() || !env.get_blob_store()) {
        return KvError::kInternalError;
    }

    // step 1: Allocate a blob for the superblock
    struct AllocCtx {
        spdk_blob_id blob_id;
        bool done;
    };
    AllocCtx alloc_ctx{SPDK_BLOBID_INVALID, false};

    env.allocate_blob(kSuperblockBlobSize, [&alloc_ctx](uint64_t blob_id) {
        alloc_ctx.blob_id = blob_id;
        alloc_ctx.done = true;
    });

    while (!alloc_ctx.done) {
        env.poll();
    }

    if (alloc_ctx.blob_id == SPDK_BLOBID_INVALID) {
        return KvError::kIoError;
    }

    superblock_blob_id_ = alloc_ctx.blob_id;

    // step 2: set as blobstore's super blob
    struct SetSuperCtx {
        int status;
        bool done;
    };
    SetSuperCtx set_ctx{0, false};

    spdk_bs_set_super(
            env.get_blob_store(), superblock_blob_id_,
            [](void* arg, int bserrno) {
                auto* ctx = static_cast<SetSuperCtx*>(arg);
                ctx->status = bserrno;
                ctx->done = true;
            },
            &set_ctx);

    while (!set_ctx.done) {
        env.poll();
    }

    if (set_ctx.status != 0) {
        return KvError::kIoError;
    }

    // step 3: open the blob
    struct OpenCtx {
        spdk_blob* blob;
        int status;
        bool done;
    };
    OpenCtx open_ctx{nullptr, 0, false};

    spdk_bs_open_blob(
            env.get_blob_store(), superblock_blob_id_,
            [](void* arg, struct spdk_blob* blob, int bserrno) {
                auto* ctx = static_cast<OpenCtx*>(arg);
                ctx->blob = blob;
                ctx->status = bserrno;
                ctx->done = true;
            },
            &open_ctx);

    while (!open_ctx.done) {
        env.poll();
    }

    if (open_ctx.status != 0 || !open_ctx.blob) {
        return KvError::kIoError;
    }

    superblock_blob_ = open_ctx.blob;
    return KvError::kSuccess;
}

KvError Engine::write_superblock() {
    auto& env = SpdkEnv::instance();
    if (!superblock_blob_ || !env.is_initialized()) {
        return KvError::kInternalError;
    }

    auto* channel = env.get_io_channel();
    if (!channel) {
        return KvError::kInternalError;
    }

    // Increment sequence and calculate checksum
    superblock_.sequence++;
    superblock_.checksum = Crc32::calculate(&superblock_, sizeof(Superblock) - sizeof(uint32_t));

    // Allocate DMA buffer and copy superblock
    size_t sb_aligned_size = align_up(sizeof(Superblock), kPageSize);
    void* write_buf = DmaAllocator::alloc_zeroed(sb_aligned_size, kPageSize);
    if (!write_buf) {
        return KvError::kInternalError;
    }

    std::memcpy(write_buf, &superblock_, sizeof(Superblock));

    uint64_t io_unit_size = spdk_bs_get_io_unit_size(env.get_blob_store());
    uint64_t length_units = sb_aligned_size / io_unit_size;

    struct WriteCtx {
        int status;
        bool done;
    };

    // write backup first (design: backup first, then primary)
    WriteCtx backup_ctx{0, false};

    spdk_blob_io_write(
            superblock_blob_, channel, write_buf, kSuperblockBackupOffset / io_unit_size,
            length_units,
            [](void* arg, int bserrno) {
                auto* ctx = static_cast<WriteCtx*>(arg);
                ctx->status = bserrno;
                ctx->done = true;
            },
            &backup_ctx);

    while (!backup_ctx.done) {
        env.poll();
    }

    if (backup_ctx.status != 0) {
        DmaAllocator::free(write_buf);
        return KvError::kIoError;
    }

    // write primary
    WriteCtx primary_ctx{0, false};

    spdk_blob_io_write(
            superblock_blob_, channel, write_buf, kSuperblockPrimaryOffset / io_unit_size,
            length_units,
            [](void* arg, int bserrno) {
                auto* ctx = static_cast<WriteCtx*>(arg);
                ctx->status = bserrno;
                ctx->done = true;
            },
            &primary_ctx);

    while (!primary_ctx.done) {
        env.poll();
    }

    DmaAllocator::free(write_buf);

    if (primary_ctx.status != 0) {
        return KvError::kIoError;
    }

    return KvError::kSuccess;
}

void Engine::update_superblock_file_mapping(FileInfo* file) {
    if (!file) {
        return;
    }

    // find existing mapping or add new one
    for (uint16_t i = 0; i < superblock_.file_count; i++) {
        if (superblock_.file_mappings[i].file_id == file->file_id) {
            superblock_.file_mappings[i].blob_id = file->blob_id;
            superblock_.file_mappings[i].size = file->size;
            superblock_.file_mappings[i].write_offset = file->write_offset;
            superblock_.file_mappings[i].state = file->state;
            return;
        }
    }

    // Add new mapping
    if (superblock_.file_count < kMaxFileCount) {
        auto& mapping = superblock_.file_mappings[superblock_.file_count];
        mapping.file_id = file->file_id;
        mapping.blob_id = file->blob_id;
        mapping.size = file->size;
        mapping.write_offset = file->write_offset;
        mapping.state = file->state;
        superblock_.file_count++;
    }
}

// --- RDMA and Buffered IO methods ---

void Engine::build_entry_inplace_rdma(void* slot, uint64_t key, uint32_t len, uint32_t seq,
                                   bool is_tombstone) {
    char* ptr = static_cast<char*>(slot);

    // Header (16 bytes)
    auto* header = reinterpret_cast<EntryHeader*>(ptr);
    header->magic = kEntryMagic;
    header->version = 1;
    header->flags = is_tombstone ? kFlagDeleted : 0;
    header->reserved = 0;
    header->sequence = seq;
    header->padding = 0;
    ptr += sizeof(EntryHeader);

    // Key (8 bytes)
    *reinterpret_cast<uint64_t*>(ptr) = key;
    ptr += sizeof(uint64_t);

    // Value Length (4 bytes)
    *reinterpret_cast<uint32_t*>(ptr) = len;
    ptr += sizeof(uint32_t);

    // Value: skip, data already in buffer via RDMA WRITE
    ptr += len;

    // calculate padding
    size_t used = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) + len;
    size_t aligned = align_up(used + sizeof(uint32_t), kPageSize);
    size_t padding = aligned - used - sizeof(uint32_t);
    if (padding > 0) {
        std::memset(ptr, 0, padding);
        ptr += padding;
    }

    // Checksum
    size_t data_size = ptr - static_cast<char*>(slot);
    *reinterpret_cast<uint32_t*>(ptr) = Crc32::calculate(slot, data_size);
}

uint32_t Engine::allocate_slot_id() { return next_global_slot_id_++; }

uint64_t Engine::get_buffer_rkey(void* /*buffer_addr*/) {
    // Placeholder for RDMA registration - returns 0 until RDMA subsystem is integrated
    return 0;
}

int Engine::alloc_rdma_slot(uint32_t value_len, RdmaSlot* out_slot) {
    size_t header_size = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t);
    size_t entry_size = header_size + value_len + sizeof(uint32_t);  // +checksum
    size_t aligned_size = align_up(entry_size, kPageSize);

    int error = 0;
    void* slot = buffer_manager_->reserve_with_backpressure(aligned_size, &error);

    if (slot == nullptr) {
        return error;
    }

    uint32_t slot_id = allocate_slot_id();
    uint32_t epoch = buffer_manager_->current_buffer_epoch();

    append_slots_[slot_id] = AppendSlot{slot, static_cast<uint32_t>(aligned_size), epoch, slot_id,
                                        AppendSlot::State::ALLOCATED};

    out_slot->buffer = slot;
    out_slot->value_offset = static_cast<uint32_t>(header_size);
    out_slot->max_value_len = static_cast<uint32_t>(aligned_size - header_size - sizeof(uint32_t));
    out_slot->rkey = get_buffer_rkey(slot);
    out_slot->slot_id = slot_id;
    out_slot->epoch = epoch;

    return 0;
}

int Engine::commit_rdma_slot(uint32_t slot_id, uint32_t epoch) {
    auto it = append_slots_.find(slot_id);
    if (it == append_slots_.end()) {
        return static_cast<int>(KvError::kInvalidArgument);
    }

    AppendSlot& slot = it->second;

    if (slot.epoch != epoch) {
        return static_cast<int>(KvError::kInvalidArgument);
    }

    if (slot.state != AppendSlot::State::ALLOCATED) {
        return static_cast<int>(KvError::kInvalidArgument);
    }

    slot.state = AppendSlot::State::RDMA_COMPLETE;

    // Notify the AppendBuffer that RDMA is complete for this buffer-local slot
    AppendBuffer* active = buffer_manager_->get_active_buffer();
    if (active) {
        active->mark_rdma_complete(slot_id);
    }

    return 0;
}

void Engine::put_rdma(uint64_t key, void* dma_buffer, uint32_t value_offset, uint32_t len,
                     uint32_t seq, KvCallback cb, void* cb_arg) {
    size_t header_size = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t);

    if (value_offset != static_cast<uint32_t>(header_size)) {
        if (cb) {
            cb(cb_arg, static_cast<int>(KvError::kInvalidArgument));
        }
        return;
    }

    // Build header and checksum (value data already in buffer via RDMA)
    build_entry_inplace_rdma(dma_buffer, key, len, seq);

    // calculate aligned size for tracking
    size_t entry_size = header_size + len + sizeof(uint32_t);
    size_t aligned_size = align_up(entry_size, kPageSize);

    uint8_t tag;
    uint64_t hash;
    HashUtil::compute_hash(key, &hash, &tag);

    // Track in buffer_pending_writes_ for the active buffer
    AppendBuffer* active = buffer_manager_->get_active_buffer();
    if (active) {
        BufferedPendingWrite bpw{};
        bpw.key = key;
        bpw.sequence = seq;
        bpw.buffer_offset = active->get_offset(dma_buffer);
        bpw.aligned_size = static_cast<uint32_t>(aligned_size);
        bpw.page_count = static_cast<uint16_t>(aligned_size / kPageSize);
        bpw.tag = tag;
        bpw.flags = 0;
        bpw.callback = cb;
        bpw.cb_arg = cb_arg;
        bpw.old_file_id = 0;
        bpw.old_offset_index = 0;
        bpw.old_garbage_size = 0;
        buffer_pending_writes_[active].push_back(bpw);
    }

    // If buffer is full, submit it
    if (active && active->is_full()) {
        buffer_manager_->submit_current_buffer();
    }
}

void Engine::put_vectored(uint64_t key, void* value, uint32_t len, KvCallback cb, void* cb_arg) {
    // Delegate to put_buffered for legacy raw-pointer callers.
    put_buffered(key, value, len, cb, cb_arg);
}

void Engine::put_buffered(uint64_t key, const void* value, uint32_t value_len, KvCallback cb,
                         void* cb_arg) {
    if (state_ != EngineState::kReady) {
        if (cb) {
            cb(cb_arg, static_cast<int>(KvError::kEngineNotReady));
        }
        return;
    }

    if (value_len == 0 || !value) {
        if (cb) {
            cb(cb_arg, static_cast<int>(KvError::kInvalidArgument));
        }
        return;
    }

    // calculate entry size
    size_t header_size = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t);
    size_t entry_size = header_size + value_len + sizeof(uint32_t);  // +checksum
    size_t aligned_size = align_up(entry_size, kPageSize);

    // Try to reserve space with backpressure check
    int error = 0;
    void* slot = buffer_manager_->reserve_with_backpressure(aligned_size, &error);

    if (slot == nullptr) {
        if (error == static_cast<int>(KvError::kBackpressure)) {
            // Backpressure active: copy data to DMA buffer and enqueue to wait queue
            void* dma_buf = DmaAllocator::alloc_zeroed(aligned_size, kPageSize);
            if (!dma_buf) {
                if (cb) {
                    cb(cb_arg, static_cast<int>(KvError::kInternalError));
                }
                return;
            }

            uint32_t seq = mem_index_->allocate_sequence();
            build_entry_inplace(dma_buf, key, value, value_len, seq);

            uint8_t tag;
            uint64_t hash;
            HashUtil::compute_hash(key, &hash, &tag);

            WaitQueueEntry wqe{};
            wqe.key = key;
            wqe.dma_buffer = dma_buf;
            wqe.value_len = value_len;
            wqe.aligned_size = static_cast<uint32_t>(aligned_size);
            wqe.sequence = seq;
            wqe.page_count = static_cast<uint16_t>(aligned_size / kPageSize);
            wqe.tag = tag;
            wqe.callback = cb;
            wqe.cb_arg = cb_arg;
            wqe.is_delete = false;
            wqe.old_garbage_size = 0;
            wait_queue_.push(wqe);

            buffer_manager_->register_resume_callback(on_backpressure_resume, this);
            return;
        }

        // Buffer full but no backpressure: submit current buffer and retry
        buffer_manager_->submit_current_buffer();

        slot = buffer_manager_->reserve_with_backpressure(aligned_size, &error);
        if (slot == nullptr) {
            // Still failed: enqueue to wait queue
            void* dma_buf = DmaAllocator::alloc_zeroed(aligned_size, kPageSize);
            if (!dma_buf) {
                if (cb) {
                    cb(cb_arg, static_cast<int>(KvError::kInternalError));
                }
                return;
            }

            uint32_t seq = mem_index_->allocate_sequence();
            build_entry_inplace(dma_buf, key, value, value_len, seq);

            uint8_t tag;
            uint64_t hash;
            HashUtil::compute_hash(key, &hash, &tag);

            WaitQueueEntry wqe{};
            wqe.key = key;
            wqe.dma_buffer = dma_buf;
            wqe.value_len = value_len;
            wqe.aligned_size = static_cast<uint32_t>(aligned_size);
            wqe.sequence = seq;
            wqe.page_count = static_cast<uint16_t>(aligned_size / kPageSize);
            wqe.tag = tag;
            wqe.callback = cb;
            wqe.cb_arg = cb_arg;
            wqe.is_delete = false;
            wqe.old_garbage_size = 0;
            wait_queue_.push(wqe);

            if (error == static_cast<int>(KvError::kBackpressure)) {
                buffer_manager_->register_resume_callback(on_backpressure_resume, this);
            }
            return;
        }
    }

    // reserve succeeded: build entry in-place
    uint32_t seq = mem_index_->allocate_sequence();
    build_entry_inplace(slot, key, value, value_len, seq);

    uint8_t tag;
    uint64_t hash;
    HashUtil::compute_hash(key, &hash, &tag);

    // Track in buffer_pending_writes_ for the active buffer
    AppendBuffer* active = buffer_manager_->get_active_buffer();
    if (active) {
        BufferedPendingWrite bpw{};
        bpw.key = key;
        bpw.sequence = seq;
        bpw.buffer_offset = active->get_offset(slot);
        bpw.aligned_size = static_cast<uint32_t>(aligned_size);
        bpw.page_count = static_cast<uint16_t>(aligned_size / kPageSize);
        bpw.tag = tag;
        bpw.flags = 0;
        bpw.callback = cb;
        bpw.cb_arg = cb_arg;
        bpw.old_file_id = 0;
        bpw.old_offset_index = 0;
        bpw.old_garbage_size = 0;
        buffer_pending_writes_[active].push_back(bpw);
    }

    // If buffer is full, submit it
    if (active && active->is_full()) {
        buffer_manager_->submit_current_buffer();
    }
}

void Engine::submit_buffer_io(AppendBuffer* buffer) {
    if (!buffer || buffer->is_empty()) {
        return;
    }

    // get active file; if full, seal and allocate new
    FileInfo* file = get_active_file();
    if (!file || !file->blob_opened ||
        file->write_offset + buffer->used() > config_.data_file_size) {
        if (file) {
            file->state = FileState::kSealed;
        }
        file = allocate_new_file();
        if (!file) {
            // Fail all pending writes for this buffer
            auto it = buffer_pending_writes_.find(buffer);
            if (it != buffer_pending_writes_.end()) {
                for (auto& bpw : it->second) {
                    if (bpw.callback) {
                        bpw.callback(bpw.cb_arg, static_cast<int>(KvError::kNoSpace));
                    }
                }
                buffer_pending_writes_.erase(it);
            }
            buffer_manager_->return_buffer(buffer);
            return;
        }
        active_file_id_ = file->file_id;
    }

    uint32_t base_page_offset = static_cast<uint32_t>(file->write_offset / kPageSize);
    uint64_t write_offset = file->write_offset;

    file->write_offset += buffer->used();
    file->size = file->write_offset;
    total_data_bytes_ += buffer->used();

    // create completion context from pool
    auto* ctx = buffer_io_ctx_pool_.alloc(this, buffer, file->file_id, base_page_offset);

    pending_foreground_count_++;

    submit_blob_write(file, write_offset, buffer->data(), static_cast<uint32_t>(buffer->used()),
                    [ctx](int status) { ctx->engine->on_buffer_io_complete(status, ctx); });
}

void Engine::on_buffer_io_complete(int status, BufferIoContext* ctx) {
    pending_foreground_count_--;

    auto it = buffer_pending_writes_.find(ctx->buffer);
    if (it != buffer_pending_writes_.end()) {
        for (auto& bpw : it->second) {
            if (status == 0) {
                uint32_t offset_index = ctx->base_page_offset +
                                        bpw.buffer_offset / static_cast<uint32_t>(kPageSize);

                MemIndexEntry entry;
                entry.key = bpw.key;
                entry.file_id = ctx->file_id;
                entry.offset_index = offset_index;
                entry.page_count = bpw.page_count;
                entry.deleted = 0;
                entry.sequence = bpw.sequence;
                entry.tag = bpw.tag;
                entry.reserved = 0;

                MemIndexEntry* existing = mem_index_->find(bpw.key);
                if (existing) {
                    uint64_t old_size = existing->page_count * kPageSize;
                    total_garbage_bytes_ += old_size;
                }
                mem_index_->upsert(bpw.key, entry);
            }

            if (bpw.callback) {
                bpw.callback(bpw.cb_arg, status == 0 ? 0 : static_cast<int>(KvError::kIoError));
            }
        }
        buffer_pending_writes_.erase(it);
    }

    buffer_manager_->return_buffer(ctx->buffer);
    buffer_io_ctx_pool_.free(ctx);
}

void Engine::on_backpressure_resume(void* arg) {
    auto* engine = static_cast<Engine*>(arg);
    engine->process_wait_queue();
}

void Engine::process_wait_queue() {
    while (!wait_queue_.empty()) {
        if (buffer_manager_->is_backpressure_active()) {
            // Re-register for resume notification
            buffer_manager_->register_resume_callback(on_backpressure_resume, this);
            break;
        }

        WaitQueueEntry wqe = wait_queue_.front();
        wait_queue_.pop();

        // The entry was pre-built in the DMA buffer at enqueue time.
        // Now we need to submit it as a direct write (like put_async path).
        FileInfo* file = get_active_file();
        bool need_new_file = !file || !file->blob_opened ||
                             file->write_offset + wqe.aligned_size > config_.data_file_size;

        if (need_new_file || allocating_new_file_) {
            PendingWriteRequest req{};
            fill_pending_write_req(req, wqe);
            pending_write_queue_.push_back(req);

            if (!allocating_new_file_) {
                if (file) {
                    file->state = FileState::kSealed;
                }
                allocate_new_file_async();
            }
            continue;
        }

        // Normal path: submit directly
        MemIndexEntry entry;
        entry.key = wqe.key;
        entry.file_id = file->file_id;
        entry.offset_index = static_cast<uint32_t>(file->write_offset / kPageSize);
        entry.page_count = wqe.page_count;
        entry.deleted = wqe.is_delete ? 1 : 0;
        entry.sequence = wqe.sequence;
        entry.tag = wqe.tag;
        entry.reserved = 0;

        uint64_t write_offset = file->write_offset;

        file->write_offset += wqe.aligned_size;
        file->size = file->write_offset;
        total_data_bytes_ += wqe.aligned_size;

        pending_foreground_count_++;

        uint64_t key = wqe.key;
        void* dma_buffer = wqe.dma_buffer;
        bool is_delete = wqe.is_delete;
        uint64_t old_garbage_size = wqe.old_garbage_size;
        KvCallback cb = wqe.callback;
        void* cb_arg = wqe.cb_arg;

        submit_blob_write(file, write_offset, dma_buffer, wqe.aligned_size,
                        [this, key, entry, dma_buffer, is_delete, old_garbage_size, cb,
                         cb_arg](int status) {
                            DmaAllocator::free(dma_buffer);
                            pending_foreground_count_--;

                            if (status == 0) {
                                if (is_delete) {
                                    mem_index_->remove(key);
                                    total_garbage_bytes_ += old_garbage_size;
                                } else {
                                    MemIndexEntry* existing = mem_index_->find(key);
                                    if (existing) {
                                        uint64_t old_size = existing->page_count * kPageSize;
                                        total_garbage_bytes_ += old_size;
                                    }
                                    mem_index_->upsert(key, entry);
                                }
                            }

                            if (cb) {
                                cb(cb_arg, status == 0 ? 0 : static_cast<int>(KvError::kIoError));
                            }
                        });
    }
}

void Engine::update_index_on_write_complete(uint64_t key, const MemIndexEntry& entry,
                                        bool is_compaction, uint16_t old_file_id,
                                        uint32_t old_offset_index) {
    MemIndexEntry* existing = mem_index_->find(key);

    if (existing == nullptr) {
        mem_index_->upsert(key, entry);
        return;
    }

    if (is_compaction) {
        // Compaction: only update if index still points to the old location.
        // This prevents compaction from overwriting a newer user write.
        if (existing->file_id == old_file_id && existing->offset_index == old_offset_index) {
            mem_index_->upsert(key, entry);
        }
    } else {
        // User write: use sequence comparison (upsert handles this internally)
        mem_index_->upsert(key, entry);
    }
}

// =========================================================================
// AllocLog Integration
// =========================================================================

void Engine::init_alloc_log_manager() {
    alloc_log_manager_ = std::make_unique<AllocLogManager>();

    auto& env = SpdkEnv::instance();
    spdk_io_channel* channel = env.is_initialized() ? env.get_io_channel() : nullptr;
    spdk_blob_store* bs = env.is_initialized() ? env.get_blob_store() : nullptr;

    alloc_log_manager_->initialize(superblock_blob_, channel, bs, superblock_.alloc_log_head,
                                   superblock_.alloc_log_tail, superblock_.alloc_log_sequence);
}

void Engine::process_alloc_log_recovery() {
    if (!alloc_log_manager_ || !alloc_log_manager_->is_initialized()) {
        return;
    }

    auto& env = SpdkEnv::instance();
    if (!env.is_initialized()) {
        return;
    }

    // Load AllocLog entries written after the last checkpoint
    bool load_done = false;
    std::vector<AllocLogEntry> alloc_entries;

    alloc_log_manager_->load_entries(
            superblock_.alloc_log_sequence,
            [&load_done, &alloc_entries](int status, const std::vector<AllocLogEntry>& entries) {
                if (status == 0) {
                    alloc_entries = entries;
                }
                load_done = true;
            });

    while (!load_done) {
        env.poll();
    }

    // For each AllocLog entry: open the blob, validate DataFileHeader.
    // If valid, add to superblock file_mappings (will be picked up by rebuild_file_info).
    // If invalid (incomplete allocation), delete the blob.
    for (const auto& entry : alloc_entries) {
        // open the blob
        struct OpenCtx {
            spdk_blob* blob;
            int status;
            bool done;
        };
        OpenCtx open_ctx{nullptr, 0, false};

        spdk_bs_open_blob(
                env.get_blob_store(), entry.blob_id,
                [](void* arg, struct spdk_blob* blob, int bserrno) {
                    auto* ctx = static_cast<OpenCtx*>(arg);
                    ctx->blob = blob;
                    ctx->status = bserrno;
                    ctx->done = true;
                },
                &open_ctx);

        while (!open_ctx.done) {
            env.poll();
        }

        if (open_ctx.status != 0 || !open_ctx.blob) {
            // Blob doesn't exist or can't be opened: skip
            continue;
        }

        // Read the DataFileHeader (first 4KB)
        void* header_buf = DmaAllocator::alloc_zeroed(kPageSize, kPageSize);
        if (!header_buf) {
            env.close_blob(open_ctx.blob, [](int) {});
            continue;
        }

        auto* channel = env.get_io_channel();
        uint64_t io_unit_size = spdk_bs_get_io_unit_size(env.get_blob_store());
        uint64_t length_units = kPageSize / io_unit_size;

        struct ReadCtx {
            int status;
            bool done;
        };
        ReadCtx read_ctx{0, false};

        spdk_blob_io_read(
                open_ctx.blob, channel, header_buf, 0, length_units,
                [](void* arg, int bserrno) {
                    auto* ctx = static_cast<ReadCtx*>(arg);
                    ctx->status = bserrno;
                    ctx->done = true;
                },
                &read_ctx);

        while (!read_ctx.done) {
            env.poll();
        }

        bool valid_header = false;
        if (read_ctx.status == 0) {
            auto* header = static_cast<DataFileHeader*>(header_buf);
            if (header->magic == kDataFileHeaderMagic) {
                uint32_t stored = header->checksum;
                uint32_t computed =
                        Crc32::calculate(header, sizeof(DataFileHeader) - sizeof(uint32_t));
                if (stored == computed && header->file_id == entry.file_id) {
                    valid_header = true;
                }
            }
        }

        DmaAllocator::free(header_buf);

        if (valid_header) {
            // Valid allocation: add to superblock file_mappings for rebuild_file_info
            if (superblock_.file_count < kMaxFileCount) {
                auto& mapping = superblock_.file_mappings[superblock_.file_count];
                mapping.file_id = entry.file_id;
                mapping.blob_id = entry.blob_id;
                mapping.size = 0;
                mapping.write_offset = sizeof(DataFileHeader);
                mapping.state = FileState::kActive;
                superblock_.file_count++;

                if (entry.file_id >= next_file_id_) {
                    next_file_id_ = entry.file_id + 1;
                }
            }

            // close blob (rebuild_file_info will reopen it)
            struct CloseCtx {
                bool done;
            };
            CloseCtx close_ctx{false};
            env.close_blob(open_ctx.blob, [&close_ctx](int) { close_ctx.done = true; });
            while (!close_ctx.done) {
                env.poll();
            }
        } else {
            // Invalid/incomplete allocation: close and delete the blob
            struct CloseCtx {
                bool done;
            };
            CloseCtx close_ctx{false};
            env.close_blob(open_ctx.blob, [&close_ctx](int) { close_ctx.done = true; });
            while (!close_ctx.done) {
                env.poll();
            }

            struct DeleteCtx {
                bool done;
            };
            DeleteCtx delete_ctx{false};
            env.delete_blob(entry.blob_id, [&delete_ctx](int) { delete_ctx.done = true; });
            while (!delete_ctx.done) {
                env.poll();
            }
        }
    }
}

void Engine::release_dangling_blobs() {
    auto& env = SpdkEnv::instance();
    if (!env.is_initialized() || !env.get_blob_store()) {
        return;
    }

    // Build the set of known blob IDs
    std::unordered_map<uint64_t, bool> known_blobs;

    // Superblock blob
    if (superblock_blob_id_ != SPDK_BLOBID_INVALID) {
        known_blobs[superblock_blob_id_] = true;
    }

    // MemIndex blobs
    if (superblock_.mem_index_blob_a != 0) {
        known_blobs[superblock_.mem_index_blob_a] = true;
    }
    if (superblock_.mem_index_blob_b != 0) {
        known_blobs[superblock_.mem_index_blob_b] = true;
    }

    // All file mapping blobs
    for (uint16_t i = 0; i < superblock_.file_count; i++) {
        known_blobs[superblock_.file_mappings[i].blob_id] = true;
    }

    // Iterate all blobs in blobstore and collect dangling ones.
    // spdk_bs_iter_first/next: callback receives each blob in turn.
    // Must call iter_next from within callback to continue.
    std::vector<spdk_blob_id> dangling_blobs;

    struct IterState {
        spdk_blob_store* bs;
        std::unordered_map<uint64_t, bool>* known;
        std::vector<spdk_blob_id>* dangling;
        bool done;
    };
    IterState iter_state{env.get_blob_store(), &known_blobs, &dangling_blobs, false};

    // Forward-declare the callback as a plain function for self-referential iter_next
    struct BlobIterHelper {
        static void Callback(void* arg, struct spdk_blob* blob, int bserrno) {
            auto* state = static_cast<IterState*>(arg);

            if (bserrno == -ENOENT || !blob) {
                state->done = true;
                return;
            }

            if (bserrno != 0) {
                state->done = true;
                return;
            }

            spdk_blob_id blob_id = spdk_blob_get_id(blob);
            if (state->known->find(blob_id) == state->known->end()) {
                state->dangling->push_back(blob_id);
            }

            // Continue to next blob
            spdk_bs_iter_next(state->bs, blob, BlobIterHelper::Callback, arg);
        }
    };

    spdk_bs_iter_first(env.get_blob_store(), BlobIterHelper::Callback, &iter_state);

    while (!iter_state.done) {
        env.poll();
    }

    // release each dangling blob
    for (auto blob_id : dangling_blobs) {
        struct DeleteCtx {
            bool done;
        };
        DeleteCtx del_ctx{false};
        env.delete_blob(blob_id, [&del_ctx](int) { del_ctx.done = true; });
        while (!del_ctx.done) {
            env.poll();
        }
    }
}

void Engine::on_checkpoint_for_alloc_complete(void* arg, int status) {
    auto* engine = static_cast<Engine*>(arg);
    engine->resume_allocation_after_checkpoint(status);
}

void Engine::resume_allocation_after_checkpoint(int status) {
    pending_checkpoint_for_alloc_ = false;

    if (status != 0) {
        // Checkpoint failed: fail all pending writes and reset allocation state
        allocating_new_file_ = false;
        for (auto& req : pending_write_queue_) {
            if (!req.is_segment_write) {
                DmaAllocator::free(req.dma_buffer);
            }
            if (req.cb) {
                req.cb(req.cb_arg, static_cast<int>(KvError::kInternalError));
            }
        }
        pending_write_queue_.clear();
        return;
    }

    // AllocLog reclaimed by checkpoint, now proceed with actual file allocation
    allocate_new_file_async();
}

// =========================================================================
// Extracted named methods (from long lambdas)
// =========================================================================

void Engine::on_superblock_update(uint32_t checkpoint_seq, const ActiveBufferPos* positions,
                                uint8_t count, std::function<void(int)> on_complete) {
    superblock_.checkpoint_global_seq = checkpoint_seq;
    superblock_.checkpoint_sequence = checkpoint_manager_->get_checkpoint_sequence() + 1;
    superblock_.active_mem_index_area = superblock_.active_mem_index_area == 0 ? 1 : 0;
    superblock_.active_file_id = active_file_id_;
    superblock_.total_entries = mem_index_ ? mem_index_->size() : 0;
    superblock_.total_data_bytes = total_data_bytes_;
    superblock_.total_garbage_bytes = total_garbage_bytes_;

    superblock_.active_buffer_count = count;
    for (uint8_t i = 0; i < count && i < kMaxBufferCount; i++) {
        superblock_.active_buffer_positions[i] = positions[i];
    }

    if (count > 0) {
        superblock_.checkpoint_file_id = positions[0].file_id;
        superblock_.checkpoint_page_index = positions[0].page_index;
    }

    for (const auto& f : files_) {
        update_superblock_file_mapping(f.get());
    }

    // reclaim AllocLog entries (all allocations are now in file_mappings)
    if (alloc_log_manager_) {
        alloc_log_manager_->reclaim();
        superblock_.alloc_log_head = alloc_log_manager_->get_head();
        superblock_.alloc_log_tail = alloc_log_manager_->get_tail();
        superblock_.alloc_log_sequence = alloc_log_manager_->get_sequence();
    }

    KvError err = write_superblock();
    if (on_complete) {
        on_complete(err == KvError::kSuccess ? 0 : -1);
    }
}

void Engine::on_queued_write_complete(int status, QueuedWriteCompletionCtx* ctx) {
    pending_foreground_count_--;

    if (status == 0) {
        if (ctx->is_delete) {
            mem_index_->remove(ctx->key);
            total_garbage_bytes_ += ctx->old_garbage_size;
        } else {
            MemIndexEntry* existing = mem_index_->find(ctx->key);
            if (existing) {
                uint64_t old_size = existing->page_count * kPageSize;
                total_garbage_bytes_ += old_size;
            }
            mem_index_->upsert(ctx->key, ctx->entry);
        }
    }

    if (ctx->cb) {
        ctx->cb(ctx->cb_arg, status == 0 ? 0 : static_cast<int>(KvError::kIoError));
    }
    queued_write_ctx_pool_.free(ctx);
}

void Engine::on_put_async_write_complete(int status, PutAsyncWriteCompletionCtx* ctx) {
    pending_foreground_count_--;

    if (status == 0) {
        MemIndexEntry* existing = mem_index_->find(ctx->key);
        if (existing) {
            uint64_t old_size = existing->page_count * kPageSize;
            total_garbage_bytes_ += old_size;
        }
        mem_index_->upsert(ctx->key, ctx->entry);
    }

    if (ctx->cb) {
        ctx->cb(ctx->cb_arg, status == 0 ? 0 : static_cast<int>(KvError::kIoError));
    }
    put_async_ctx_pool_.free(ctx);
}

void Engine::on_compaction_blob_closed(bool close_ok, uint16_t file_id, FileInfo* file,
                                    std::function<void(bool)> callback) {
    if (!close_ok) {
        if (callback) {
            callback(false);
        }
        return;
    }

    auto& env = SpdkEnv::instance();
    if (!env.is_initialized()) {
        if (callback) {
            callback(false);
        }
        return;
    }

    // del the blob
    env.delete_blob(file->blob_id, [this, file_id, callback](int status) {
        // Clean up metadata
        file_metadata_.erase(file_id);

        if (callback) {
            callback(status == 0);
        }
    });
}

void Engine::on_blob_allocated(uint64_t blob_id, FileInfo* file, std::function<void(bool)> callback) {
    if (blob_id == SPDK_BLOBID_INVALID) {
        if (callback) {
            callback(false);
        }
        return;
    }

    file->blob_id = blob_id;

    // write AllocLog entry BEFORE opening blob / writing DataFileHeader.
    // This ensures crash consistency: if we crash after AllocLog but before
    // DataFileHeader, recovery can identify this blob via AllocLog.
    if (alloc_log_manager_ && alloc_log_manager_->is_initialized()) {
        alloc_log_manager_->write_entry(blob_id, file->file_id, config_.data_file_size,
                                       [this, file, callback](int status) {
                                           if (status != 0) {
                                               if (callback) {
                                                   callback(false);
                                               }
                                               return;
                                           }
                                           // AllocLog persisted, now open blob and write
                                           // DataFileHeader
                                           open_blob_for_file(file, callback);
                                       });
    } else {
        // No AllocLog manager: legacy path
        open_blob_for_file(file, callback);
    }
}

// C API implementation
extern "C" {

int spdk_kv_create(const char* path, struct spdk_kv_create_opts* opts, spdk_kv_handle* handle) {
    auto* engine = new Engine();
    CreateOpts create_opts;
    if (opts) {
        create_opts.config.max_capacity = opts->max_capacity;
        create_opts.config.data_file_size = opts->data_file_size;
        create_opts.config.max_entries = opts->max_entries;
        create_opts.config.index_load_factor = opts->index_load_factor;
        create_opts.force = opts->force != 0;
    }

    KvError err = engine->create(path ? path : "", create_opts);
    if (err != KvError::kSuccess) {
        delete engine;
        return static_cast<int>(err);
    }

    *handle = engine;
    return 0;
}

int spdk_kv_open(const char* path, struct spdk_kv_open_opts* opts, spdk_kv_handle* handle) {
    auto* engine = new Engine();
    OpenOpts open_opts;
    if (opts) {
        open_opts.read_only = opts->read_only != 0;
        open_opts.recover = opts->recover != 0;
    }

    KvError err = engine->open(path ? path : "", open_opts);
    if (err != KvError::kSuccess) {
        delete engine;
        return static_cast<int>(err);
    }

    *handle = engine;
    return 0;
}

int spdk_kv_close(spdk_kv_handle handle) {
    if (!handle) {
        return -1;
    }
    auto* engine = static_cast<Engine*>(handle);
    KvError err = engine->close();
    delete engine;
    return static_cast<int>(err);
}

int spdk_kv_put(spdk_kv_handle handle, uint64_t key, const void* value, uint32_t value_len) {
    if (!handle) {
        return -1;
    }
    auto* engine = static_cast<Engine*>(handle);
    return static_cast<int>(engine->put(key, value, value_len));
}

int spdk_kv_get(spdk_kv_handle handle, uint64_t key, void* value_buf, uint32_t buf_len,
                uint32_t* actual_len) {
    if (!handle) {
        return -1;
    }
    auto* engine = static_cast<Engine*>(handle);
    return static_cast<int>(engine->get(key, value_buf, buf_len, actual_len));
}

int spdk_kv_del(spdk_kv_handle handle, uint64_t key) {
    if (!handle) {
        return -1;
    }
    auto* engine = static_cast<Engine*>(handle);
    return static_cast<int>(engine->del(key));
}

void spdk_kv_put_async(spdk_kv_handle handle, uint64_t key, const void* value, uint32_t value_len,
                       spdk_kv_cb cb, void* cb_arg) {
    if (!handle) {
        if (cb) {
            cb(cb_arg, -1);
        }
        return;
    }
    auto* engine = static_cast<Engine*>(handle);
    engine->put_buffered(key, value, value_len, cb, cb_arg);
}

// C API wrapper context for get_async
struct CApiGetAsyncCtx {
    SegmentBuf output;
    void* dma_buffer;       // DMA buffer allocated for reading
    uint32_t dma_buf_size;  // size of allocated DMA buffer
    void* user_buf;
    uint32_t user_buf_len;
    spdk_kv_get_cb user_cb;
    void* user_cb_arg;
};

static CtxPool<CApiGetAsyncCtx, 64> g_capi_get_ctx_pool;

static void c_api_get_async_callback(void* arg, int status, uint32_t actual_len) {
    auto* ctx = static_cast<CApiGetAsyncCtx*>(arg);

    if (status == 0) {
        // Check if user buffer is large enough for actual value
        if (actual_len > ctx->user_buf_len) {
            // free DMA buffer
            DmaAllocator::free(ctx->dma_buffer);
            if (ctx->user_cb) {
                ctx->user_cb(ctx->user_cb_arg, static_cast<int>(KvError::kValueTooLarge),
                             actual_len);
            }
            g_capi_get_ctx_pool.free(ctx);
            return;
        }

        // Copy value data (skip first 512 bytes header) to user buffer
        // data layout: [0-512) header, [512-...) actual value
        char* src = static_cast<char*>(ctx->dma_buffer) + kSegmentDataOffset;
        std::memcpy(ctx->user_buf, src, actual_len);
    }

    // free DMA buffer
    if (ctx->dma_buffer) {
        DmaAllocator::free(ctx->dma_buffer);
    }

    if (ctx->user_cb) {
        ctx->user_cb(ctx->user_cb_arg, status, actual_len);
    }
    g_capi_get_ctx_pool.free(ctx);
}

void spdk_kv_get_async(spdk_kv_handle handle, uint64_t key, void* value_buf, uint32_t buf_len,
                       spdk_kv_get_cb cb, void* cb_arg) {
    if (!handle) {
        if (cb) {
            cb(cb_arg, -1, 0);
        }
        return;
    }

    // Allocate DMA buffer: user buffer size + 512 byte header, aligned up to 4KB
    // Use maximum of 16 pages (64KB) as upper bound for SegmentBuf capacity
    uint32_t required_size =
            align_up(static_cast<uint64_t>(buf_len) + kSegmentDataOffset, kPageSize);
    uint32_t max_size = 16 * kPageSize;  // SegmentBuf can hold at most 16 pages
    uint32_t alloc_size = std::min(required_size, max_size);

    void* dma_buffer = DmaAllocator::alloc(alloc_size, kPageSize);
    if (!dma_buffer) {
        if (cb) {
            cb(cb_arg, static_cast<int>(KvError::kInternalError), 0);
        }
        return;
    }

    auto* ctx = g_capi_get_ctx_pool.alloc();
    ctx->dma_buffer = dma_buffer;
    ctx->dma_buf_size = alloc_size;
    ctx->user_buf = value_buf;
    ctx->user_buf_len = buf_len;
    ctx->user_cb = cb;
    ctx->user_cb_arg = cb_arg;

    // Setup SegmentBuf with single contiguous DMA buffer
    ctx->output.cnt_ = 1;
    ctx->output.buffers_[0].iov_base = dma_buffer;
    ctx->output.buffers_[0].iov_len = alloc_size;

    auto* engine = static_cast<Engine*>(handle);
    engine->get_async(key, &ctx->output, c_api_get_async_callback, ctx);
}

void spdk_kv_del_async(spdk_kv_handle handle, uint64_t key, spdk_kv_cb cb, void* cb_arg) {
    if (!handle) {
        if (cb) {
            cb(cb_arg, -1);
        }
        return;
    }
    auto* engine = static_cast<Engine*>(handle);
    engine->delete_async(key, cb, cb_arg);
}

void spdk_kv_poll(spdk_kv_handle handle) {
    if (!handle) {
        return;
    }
    auto* engine = static_cast<Engine*>(handle);
    engine->poll();
}

uint64_t spdk_kv_get_entry_count(spdk_kv_handle handle) {
    if (!handle) {
        return 0;
    }
    auto* engine = static_cast<Engine*>(handle);
    return engine->get_entry_count();
}

uint64_t spdk_kv_get_total_bytes(spdk_kv_handle handle) {
    if (!handle) {
        return 0;
    }
    auto* engine = static_cast<Engine*>(handle);
    return engine->get_total_data_bytes();
}

}  // extern "C"

}  // namespace spdk_kv
