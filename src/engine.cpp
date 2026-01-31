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
          pending_blob_ops_(0),
          superblock_blob_(nullptr),
          superblock_blob_id_(SPDK_BLOBID_INVALID) {
    std::memset(&superblock_, 0, sizeof(superblock_));
}

Engine::~Engine() {
    if (state_ == EngineState::kReady) {
        Close();
    }
}

KvError Engine::Create(const std::string& path, const CreateOpts& opts) {
    if (state_ != EngineState::kUninitialized) {
        return KvError::kInvalidState;
    }

    state_ = EngineState::kOpening;
    path_ = path;
    config_ = opts.config;

    KvError err = InitializeNew(opts);
    if (err != KvError::kSuccess) {
        state_ = EngineState::kError;
        return err;
    }

    state_ = EngineState::kReady;
    return KvError::kSuccess;
}

KvError Engine::Open(const std::string& path, const OpenOpts& opts) {
    if (state_ != EngineState::kUninitialized) {
        return KvError::kInvalidState;
    }

    state_ = EngineState::kOpening;
    path_ = path;

    KvError err = LoadExisting(opts);
    if (err != KvError::kSuccess) {
        state_ = EngineState::kError;
        return err;
    }

    if (opts.recover) {
        state_ = EngineState::kRecovering;
        err = Recover();
        if (err != KvError::kSuccess) {
            state_ = EngineState::kError;
            return err;
        }
    }

    state_ = EngineState::kReady;
    return KvError::kSuccess;
}

KvError Engine::Close() {
    if (state_ != EngineState::kReady) {
        return KvError::kInvalidState;
    }

    state_ = EngineState::kClosing;

    // Flush any pending writes
    if (buffer_manager_) {
        buffer_manager_->SubmitCurrentBuffer();
    }

    // Update superblock with latest state before persisting
    superblock_.active_file_id = active_file_id_;
    superblock_.total_data_bytes = total_data_bytes_;
    superblock_.total_garbage_bytes = total_garbage_bytes_;
    superblock_.total_entries = mem_index_ ? mem_index_->Size() : 0;

    // Update file mappings with current state
    for (const auto& file : files_) {
        UpdateSuperblockFileMapping(file.get());
    }

    // Persist superblock
    if (superblock_blob_) {
        WriteSuperblock();
    }

    // Close all open data blobs
    for (auto& file : files_) {
        if (file->blob_opened && file->blob) {
            CloseBlobForFile(file.get(), nullptr);
        }
    }

    // Wait for pending blob operations to complete
    while (pending_blob_ops_ > 0) {
        if (io_submitter_) {
            io_submitter_->ProcessCompletions(32);
        }
        SpdkEnv::Instance().Poll();
    }

    // Close superblock blob
    if (superblock_blob_) {
        auto& env = SpdkEnv::Instance();
        if (env.IsInitialized()) {
            struct CloseCtx {
                bool done;
            };
            CloseCtx close_ctx{false};

            env.CloseBlob(superblock_blob_, [&close_ctx](int) { close_ctx.done = true; });

            while (!close_ctx.done) {
                env.Poll();
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

    state_ = EngineState::kClosed;
    return KvError::kSuccess;
}

KvError Engine::InitializeNew(const CreateOpts& opts) {
    (void)opts;  // Suppress unused parameter warning (used for future extension)
    // Initialize memory index
    mem_index_ = std::make_unique<MemIndex>(config_.max_entries, config_.index_load_factor);
    if (!mem_index_) {
        return KvError::kInternalError;
    }

    // Initialize append buffer manager
    buffer_manager_ = std::make_unique<AppendBufferManager>();
    if (!buffer_manager_->Initialize(config_.append_buffer_count, config_.append_buffer_size)) {
        return KvError::kInternalError;
    }

    // Initialize checkpoint manager
    checkpoint_manager_ =
            std::make_unique<IncrementalCheckpoint>(mem_index_.get(), mem_index_->Capacity());

    // Initialize compaction scheduler
    compaction_scheduler_ = std::make_unique<CompactionScheduler>(mem_index_.get());
    compaction_scheduler_->SetFileMetadata(&file_metadata_);

    // Initialize IO submitter
    io_submitter_ = std::make_unique<IoSubmitter>();

    // In SPDK mode, initialize with SPDK resources
    auto& env = SpdkEnv::Instance();
    if (env.IsInitialized()) {
        io_submitter_->Initialize(env.GetController(), env.GetNamespace(), env.GetBlobStore());
    } else {
        return KvError::kInternalError;
    }

    // Initialize superblock in memory (before AllocateNewFile, which updates file_mappings)
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

    // Create first data file (this calls UpdateSuperblockFileMapping internally)
    FileInfo* file = AllocateNewFile();
    if (!file) {
        return KvError::kInternalError;
    }
    active_file_id_ = file->file_id;
    superblock_.active_file_id = active_file_id_;

    // Create superblock blob and persist to NVMe
    KvError sb_err = CreateSuperblockBlob();
    if (sb_err != KvError::kSuccess) {
        return sb_err;
    }

    sb_err = WriteSuperblock();
    if (sb_err != KvError::kSuccess) {
        return sb_err;
    }

    return KvError::kSuccess;
}

KvError Engine::LoadExisting(const OpenOpts& opts) {
    // Step 1: Load superblock from persistent storage
    KvError err = LoadSuperblock();
    if (err != KvError::kSuccess) {
        return err;
    }

    // Step 2: Validate superblock
    if (!superblock_.is_valid()) {
        return KvError::kCorruption;
    }

    // Step 3: Load configuration from superblock
    config_.max_capacity = superblock_.total_capacity;
    config_.data_file_size = superblock_.data_file_size;
    // Keep other config values at defaults or load from persistent storage if available

    // Step 4: Initialize memory index
    mem_index_ = std::make_unique<MemIndex>(config_.max_entries, config_.index_load_factor);
    if (!mem_index_) {
        return KvError::kInternalError;
    }

    // Step 5: Initialize append buffer manager
    buffer_manager_ = std::make_unique<AppendBufferManager>();
    if (!buffer_manager_->Initialize(config_.append_buffer_count, config_.append_buffer_size)) {
        return KvError::kInternalError;
    }

    // Step 6: Initialize IO submitter
    io_submitter_ = std::make_unique<IoSubmitter>();
    auto& env = SpdkEnv::Instance();
    if (env.IsInitialized()) {
        io_submitter_->Initialize(env.GetController(), env.GetNamespace(), env.GetBlobStore());
    } else {
        return KvError::kInternalError;
    }

    // Step 7: Rebuild file info from superblock
    err = RebuildFileInfo();
    if (err != KvError::kSuccess) {
        return err;
    }

    // Step 8: Initialize checkpoint manager
    checkpoint_manager_ =
            std::make_unique<IncrementalCheckpoint>(mem_index_.get(), mem_index_->Capacity());

    // Step 9: Initialize compaction scheduler
    compaction_scheduler_ = std::make_unique<CompactionScheduler>(mem_index_.get());
    compaction_scheduler_->SetFileMetadata(&file_metadata_);

    // Step 10: Restore statistics from superblock
    total_data_bytes_ = superblock_.total_data_bytes;
    total_garbage_bytes_ = superblock_.total_garbage_bytes;
    active_file_id_ = superblock_.active_file_id;
    next_file_id_ = superblock_.file_count;

    // Step 11: Restore global sequence number from checkpoint
    // This will be updated during recovery if needed
    mem_index_->SetGlobalSequence(superblock_.checkpoint_global_seq);

    // Step 12: Perform recovery if requested
    if (opts.recover) {
        err = RecoverMemIndex();
        if (err != KvError::kSuccess) {
            return err;
        }
    }

    // Step 13: Update last mount time in superblock
    auto now = std::chrono::system_clock::now().time_since_epoch();
    superblock_.last_mount_time = std::chrono::duration_cast<std::chrono::seconds>(now).count();

    return KvError::kSuccess;
}

KvError Engine::LoadSuperblock() {
    auto& env = SpdkEnv::Instance();
    if (!env.IsInitialized() || !env.GetBlobStore()) {
        return KvError::kInternalError;
    }

    // Step 1: Get super blob ID from blobstore
    struct GetSuperCtx {
        spdk_blob_id blob_id;
        int status;
        bool done;
    };
    GetSuperCtx get_ctx{SPDK_BLOBID_INVALID, 0, false};

    spdk_bs_get_super(env.GetBlobStore(),
                      [](void* arg, spdk_blob_id blobid, int bserrno) {
                          auto* ctx = static_cast<GetSuperCtx*>(arg);
                          ctx->blob_id = blobid;
                          ctx->status = bserrno;
                          ctx->done = true;
                      },
                      &get_ctx);

    while (!get_ctx.done) {
        env.Poll();
    }

    if (get_ctx.status != 0 || get_ctx.blob_id == SPDK_BLOBID_INVALID) {
        return KvError::kCorruption;
    }

    superblock_blob_id_ = get_ctx.blob_id;

    // Step 2: Open the super blob
    struct OpenCtx {
        spdk_blob* blob;
        int status;
        bool done;
    };
    OpenCtx open_ctx{nullptr, 0, false};

    spdk_bs_open_blob(env.GetBlobStore(), superblock_blob_id_,
                      [](void* arg, struct spdk_blob* blob, int bserrno) {
                          auto* ctx = static_cast<OpenCtx*>(arg);
                          ctx->blob = blob;
                          ctx->status = bserrno;
                          ctx->done = true;
                      },
                      &open_ctx);

    while (!open_ctx.done) {
        env.Poll();
    }

    if (open_ctx.status != 0 || !open_ctx.blob) {
        return KvError::kCorruption;
    }

    superblock_blob_ = open_ctx.blob;

    // Step 3: Read primary superblock
    size_t sb_aligned_size = AlignUp(sizeof(Superblock), kPageSize);
    void* read_buf = DmaAllocator::AllocZeroed(sb_aligned_size, kPageSize);
    if (!read_buf) {
        return KvError::kInternalError;
    }

    auto* channel = env.GetIoChannel();
    if (!channel) {
        DmaAllocator::Free(read_buf);
        return KvError::kInternalError;
    }

    uint64_t io_unit_size = spdk_bs_get_io_unit_size(env.GetBlobStore());
    uint64_t length_units = sb_aligned_size / io_unit_size;

    struct ReadCtx {
        int status;
        bool done;
    };
    ReadCtx read_ctx{0, false};

    spdk_blob_io_read(superblock_blob_, channel, read_buf, kSuperblockPrimaryOffset / io_unit_size,
                      length_units,
                      [](void* arg, int bserrno) {
                          auto* ctx = static_cast<ReadCtx*>(arg);
                          ctx->status = bserrno;
                          ctx->done = true;
                      },
                      &read_ctx);

    while (!read_ctx.done) {
        env.Poll();
    }

    if (read_ctx.status == 0) {
        // Validate primary superblock
        auto* sb = static_cast<Superblock*>(read_buf);
        if (sb->magic == kSuperblockMagic) {
            uint32_t stored_checksum = sb->checksum;
            uint32_t computed = Crc32::Calculate(sb, sizeof(Superblock) - sizeof(uint32_t));
            if (stored_checksum == computed) {
                std::memcpy(&superblock_, sb, sizeof(Superblock));
                DmaAllocator::Free(read_buf);
                return KvError::kSuccess;
            }
        }
    }

    // Step 4: Primary invalid, try backup superblock at kSuperblockBackupOffset
    ReadCtx backup_ctx{0, false};

    spdk_blob_io_read(superblock_blob_, channel, read_buf, kSuperblockBackupOffset / io_unit_size,
                      length_units,
                      [](void* arg, int bserrno) {
                          auto* ctx = static_cast<ReadCtx*>(arg);
                          ctx->status = bserrno;
                          ctx->done = true;
                      },
                      &backup_ctx);

    while (!backup_ctx.done) {
        env.Poll();
    }

    if (backup_ctx.status == 0) {
        auto* sb = static_cast<Superblock*>(read_buf);
        if (sb->magic == kSuperblockMagic) {
            uint32_t stored_checksum = sb->checksum;
            uint32_t computed = Crc32::Calculate(sb, sizeof(Superblock) - sizeof(uint32_t));
            if (stored_checksum == computed) {
                std::memcpy(&superblock_, sb, sizeof(Superblock));
                DmaAllocator::Free(read_buf);
                return KvError::kSuccess;
            }
        }
    }

    DmaAllocator::Free(read_buf);
    return KvError::kCorruption;
}

KvError Engine::RebuildFileInfo() {
    files_.clear();

    auto& env = SpdkEnv::Instance();
    if (!env.IsInitialized()) {
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

        // Initialize file metadata for compaction tracking
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

        // Open the existing blob (without writing header - data already on disk)
        pending_opens++;
        pending_blob_ops_++;
        env.OpenBlob(ptr->blob_id,
                     [this, ptr, &pending_opens, &open_error](spdk_blob* blob) {
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
        env.Poll();
    }

    if (open_error) {
        return KvError::kIoError;
    }

    // If no files exist, create the first one
    if (files_.empty()) {
        FileInfo* new_file = AllocateNewFile();
        if (!new_file) {
            return KvError::kInternalError;
        }
        active_file_id_ = new_file->file_id;
    }

    return KvError::kSuccess;
}

KvError Engine::RecoverMemIndex() {
    // Use IndexLoader to recover MemIndex from checkpoint and data files
    IndexLoader loader(mem_index_.get());

    // Set the superblock for the loader
    loader.SetSuperblock(superblock_);

    // Prepare file data for scanning
    std::vector<IndexLoader::FileData> file_data;
    for (const auto& file : files_) {
        if (file->state != FileState::kDeleted && !file->data.empty()) {
            IndexLoader::FileData fd;
            fd.file_id = file->file_id;
            fd.size = file->size;
            fd.data = file->data.data();
            file_data.push_back(fd);
        }
    }
    loader.SetFileData(file_data);

    // Start recovery
    KvError recovery_error = KvError::kSuccess;
    loader.StartRecovery([&recovery_error](KvError status) { recovery_error = status; });

    // Poll until recovery completes
    while (loader.Poll()) {
        // Process completions while recovery is in progress
    }

    if (loader.IsSuccess()) {
        // Update global sequence from recovery
        uint32_t recovered_max_seq = loader.GetRecoveredMaxSequence();
        uint32_t checkpoint_seq = superblock_.checkpoint_global_seq;
        uint32_t new_global_seq = std::max(checkpoint_seq, recovered_max_seq) + 1;
        mem_index_->SetGlobalSequence(new_global_seq);
        return KvError::kSuccess;
    }

    return recovery_error;
}

KvError Engine::Recover() {
    // Recovery is handled by RecoverMemIndex() called from LoadExisting()
    return KvError::kSuccess;
}

FileInfo* Engine::AllocateNewFile() {
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
    AllocateBlobForFile(ptr, [&alloc_done, &alloc_success](bool success) {
        alloc_success = success;
        alloc_done = true;
    });

    while (!alloc_done) {
        SpdkEnv::Instance().Poll();
    }

    if (!alloc_success) {
        files_.pop_back();
        next_file_id_--;
        return nullptr;
    }

    // Initialize file metadata for compaction tracking
    auto& meta = file_metadata_[ptr->file_id];
    meta.file_id = ptr->file_id;
    meta.state = ptr->state;
    meta.total_entries = 0;
    meta.valid_entries = 0;
    meta.total_bytes = 0;
    meta.valid_bytes = 0;

    // Update superblock file mapping
    UpdateSuperblockFileMapping(ptr);

    return ptr;
}

FileInfo* Engine::GetActiveFile() { return GetFile(active_file_id_); }

FileInfo* Engine::GetFile(uint16_t file_id) {
    for (auto& file : files_) {
        if (file->file_id == file_id) {
            return file.get();
        }
    }
    return nullptr;
}

void Engine::BuildEntryInplace(void* slot, uint64_t key, const void* value, uint32_t len,
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

    // Calculate padding
    size_t used = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) + len;
    size_t aligned = AlignUp(used + sizeof(uint32_t), kPageSize);
    size_t padding = aligned - used - sizeof(uint32_t);
    if (padding > 0) {
        std::memset(ptr, 0, padding);
        ptr += padding;
    }

    // Checksum
    size_t data_size = ptr - static_cast<char*>(slot);
    *reinterpret_cast<uint32_t*>(ptr) = Crc32::Calculate(slot, data_size);
}

KvError Engine::Put(uint64_t key, const void* value, uint32_t value_len) {
    if (state_ != EngineState::kReady) {
        return KvError::kEngineNotReady;
    }

    if (value_len == 0) {
        return KvError::kInvalidArgument;
    }

    if (!value) {
        return KvError::kInvalidArgument;
    }

    // Calculate entry size
    size_t header_size = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t);
    size_t entry_size = header_size + value_len + sizeof(uint32_t);  // +checksum
    size_t aligned_size = AlignUp(entry_size, kPageSize);

    // Get active file
    FileInfo* file = GetActiveFile();
    if (!file || file->write_offset + aligned_size > config_.data_file_size) {
        // Seal current file and create new one
        if (file) {
            file->state = FileState::kSealed;
        }
        file = AllocateNewFile();
        if (!file) {
            return KvError::kNoSpace;
        }
        active_file_id_ = file->file_id;
    }

    // Allocate sequence number
    uint32_t seq = mem_index_->AllocateSequence();

    // Build entry in file
    void* slot = file->data.data() + file->write_offset;
    BuildEntryInplace(slot, key, value, value_len, seq);

    // Update index
    MemIndexEntry entry;
    entry.key = key;
    entry.file_id = file->file_id;
    entry.offset_index = static_cast<uint32_t>(file->write_offset / kPageSize);
    entry.page_count = static_cast<uint16_t>(aligned_size / kPageSize);
    entry.deleted = 0;
    entry.sequence = seq;

    uint64_t hash;
    HashUtil::ComputeHash(key, &hash, &entry.tag);

    // Check for existing entry (for garbage tracking)
    MemIndexEntry* existing = mem_index_->Find(key);
    if (existing) {
        // Mark old data as garbage
        uint64_t old_size = existing->page_count * kPageSize;
        total_garbage_bytes_ += old_size;
    }

    mem_index_->Upsert(key, entry);

    // Update file offset
    file->write_offset += aligned_size;
    file->size = file->write_offset;
    total_data_bytes_ += aligned_size;

    return KvError::kSuccess;
}

KvError Engine::Get(uint64_t key, void* value_buf, uint32_t buf_len, uint32_t* actual_len) {
    if (state_ != EngineState::kReady) {
        return KvError::kEngineNotReady;
    }

    if (!value_buf || buf_len == 0) {
        return KvError::kInvalidArgument;
    }

    // Find in index
    MemIndexEntry* entry = mem_index_->Find(key);
    if (!entry) {
        return KvError::kKeyNotFound;
    }

    if (entry->is_deleted()) {
        return KvError::kKeyNotFound;
    }

    // Get file
    FileInfo* file = GetFile(entry->file_id);
    if (!file || !file->IsReadable()) {
        return KvError::kIoError;
    }

    // Calculate offset
    uint64_t offset = entry->offset_index * kPageSize;

    // Read entry header
    auto* header = reinterpret_cast<EntryHeader*>(file->data.data() + offset);
    if (!header->is_valid()) {
        return KvError::kCorruption;
    }

    // Get value length
    uint32_t value_len = *reinterpret_cast<uint32_t*>(file->data.data() + offset +
                                                      sizeof(EntryHeader) + sizeof(uint64_t));

    if (actual_len) {
        *actual_len = value_len;
    }

    if (value_len > buf_len) {
        return KvError::kValueTooLarge;
    }

    // Copy value
    void* value_ptr =
            file->data.data() + offset + sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t);
    std::memcpy(value_buf, value_ptr, value_len);

    return KvError::kSuccess;
}

KvError Engine::Delete(uint64_t key) {
    if (state_ != EngineState::kReady) {
        return KvError::kEngineNotReady;
    }

    // Check if key exists
    MemIndexEntry* existing = mem_index_->Find(key);
    if (!existing) {
        return KvError::kKeyNotFound;
    }

    // Calculate tombstone size
    size_t header_size = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t);
    size_t entry_size = header_size + sizeof(uint32_t);  // +checksum
    size_t aligned_size = AlignUp(entry_size, kPageSize);

    // Get active file
    FileInfo* file = GetActiveFile();
    if (!file || file->write_offset + aligned_size > config_.data_file_size) {
        if (file) {
            file->state = FileState::kSealed;
        }
        file = AllocateNewFile();
        if (!file) {
            return KvError::kNoSpace;
        }
        active_file_id_ = file->file_id;
    }

    // Allocate sequence number
    uint32_t seq = mem_index_->AllocateSequence();

    // Build tombstone entry
    void* slot = file->data.data() + file->write_offset;
    BuildEntryInplace(slot, key, nullptr, 0, seq, true);

    // Update index
    mem_index_->Remove(key);

    // Track garbage
    uint64_t old_size = existing->page_count * kPageSize;
    total_garbage_bytes_ += old_size;

    // Update file offset
    file->write_offset += aligned_size;
    file->size = file->write_offset;
    total_data_bytes_ += aligned_size;

    return KvError::kSuccess;
}

void Engine::PutAsync(uint64_t key, const void* value, uint32_t value_len, KvCallback cb,
                      void* cb_arg) {
    if (state_ != EngineState::kReady) {
        if (cb) cb(cb_arg, static_cast<int>(KvError::kEngineNotReady));
        return;
    }

    if (value_len == 0 || !value) {
        if (cb) cb(cb_arg, static_cast<int>(KvError::kInvalidArgument));
        return;
    }

    // Calculate entry size
    size_t header_size = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t);
    size_t entry_size = header_size + value_len + sizeof(uint32_t);  // +checksum
    size_t aligned_size = AlignUp(entry_size, kPageSize);

    // Get active file
    FileInfo* file = GetActiveFile();
    if (!file || !file->blob_opened || file->write_offset + aligned_size > config_.data_file_size) {
        // Seal current file and create new one
        if (file) {
            file->state = FileState::kSealed;
        }
        file = AllocateNewFile();
        if (!file) {
            if (cb) cb(cb_arg, static_cast<int>(KvError::kNoSpace));
            return;
        }
        active_file_id_ = file->file_id;
    }

    // Allocate DMA buffer for the entry
    void* dma_buffer = DmaAllocator::AllocZeroed(aligned_size, kPageSize);
    if (!dma_buffer) {
        if (cb) cb(cb_arg, static_cast<int>(KvError::kInternalError));
        return;
    }

    // Allocate sequence number
    uint32_t seq = mem_index_->AllocateSequence();

    // Build entry in DMA buffer
    BuildEntryInplace(dma_buffer, key, value, value_len, seq);

    // Prepare index entry
    MemIndexEntry entry;
    entry.key = key;
    entry.file_id = file->file_id;
    entry.offset_index = static_cast<uint32_t>(file->write_offset / kPageSize);
    entry.page_count = static_cast<uint16_t>(aligned_size / kPageSize);
    entry.deleted = 0;
    entry.sequence = seq;

    uint64_t hash;
    HashUtil::ComputeHash(key, &hash, &entry.tag);

    uint64_t write_offset = file->write_offset;

    // Update write offset before async operation
    file->write_offset += aligned_size;
    file->size = file->write_offset;
    total_data_bytes_ += aligned_size;

    pending_foreground_count_++;

    // Submit async blob write
    SubmitBlobWrite(file, write_offset, dma_buffer, static_cast<uint32_t>(aligned_size),
                    [this, key, entry, dma_buffer, cb, cb_arg, aligned_size](int status) {
                        DmaAllocator::Free(dma_buffer);
                        pending_foreground_count_--;

                        if (status == 0) {
                            // Check for existing entry (for garbage tracking)
                            MemIndexEntry* existing = mem_index_->Find(key);
                            if (existing) {
                                uint64_t old_size = existing->page_count * kPageSize;
                                total_garbage_bytes_ += old_size;
                            }

                            // Update index on successful write
                            mem_index_->Upsert(key, entry);
                        }

                        if (cb) cb(cb_arg, status == 0 ? 0 : static_cast<int>(KvError::kIoError));
                    });
    return;
}

void Engine::GetAsync(uint64_t key, void* value_buf, uint32_t buf_len, KvGetCallback cb,
                      void* cb_arg) {
    if (state_ != EngineState::kReady) {
        if (cb) cb(cb_arg, static_cast<int>(KvError::kEngineNotReady), 0);
        return;
    }

    if (!value_buf || buf_len == 0) {
        if (cb) cb(cb_arg, static_cast<int>(KvError::kInvalidArgument), 0);
        return;
    }

    // Find in index
    MemIndexEntry* entry = mem_index_->Find(key);
    if (!entry) {
        if (cb) cb(cb_arg, static_cast<int>(KvError::kKeyNotFound), 0);
        return;
    }

    if (entry->is_deleted()) {
        if (cb) cb(cb_arg, static_cast<int>(KvError::kKeyNotFound), 0);
        return;
    }

    // Get file
    FileInfo* file = GetFile(entry->file_id);
    if (!file || !file->IsReadable() || !file->blob_opened) {
        if (cb) cb(cb_arg, static_cast<int>(KvError::kIoError), 0);
        return;
    }

    // Calculate read size (read the full entry)
    uint32_t read_pages = entry->page_count;
    uint32_t read_size = read_pages * kPageSize;

    // Allocate DMA buffer for reading
    void* dma_buffer = DmaAllocator::Alloc(read_size, kPageSize);
    if (!dma_buffer) {
        if (cb) cb(cb_arg, static_cast<int>(KvError::kInternalError), 0);
        return;
    }

    uint64_t offset = entry->offset_index * kPageSize;

    pending_foreground_count_++;

    // Submit async blob read
    SubmitBlobRead(file, offset, dma_buffer, read_size,
                   [this, dma_buffer, value_buf, buf_len, cb, cb_arg](int status) {
                       pending_foreground_count_--;

                       if (status != 0) {
                           DmaAllocator::Free(dma_buffer);
                           if (cb) cb(cb_arg, static_cast<int>(KvError::kIoError), 0);
                           return;
                       }

                       // Parse entry header
                       auto* header = static_cast<EntryHeader*>(dma_buffer);
                       if (!header->is_valid()) {
                           DmaAllocator::Free(dma_buffer);
                           if (cb) cb(cb_arg, static_cast<int>(KvError::kCorruption), 0);
                           return;
                       }

                       // Get value length
                       uint32_t value_len =
                               *reinterpret_cast<uint32_t*>(static_cast<char*>(dma_buffer) +
                                                            sizeof(EntryHeader) + sizeof(uint64_t));

                       if (value_len > buf_len) {
                           DmaAllocator::Free(dma_buffer);
                           if (cb) cb(cb_arg, static_cast<int>(KvError::kValueTooLarge), value_len);
                           return;
                       }

                       // Copy value to user buffer
                       void* value_ptr = static_cast<char*>(dma_buffer) + sizeof(EntryHeader) +
                                         sizeof(uint64_t) + sizeof(uint32_t);
                       std::memcpy(value_buf, value_ptr, value_len);

                       DmaAllocator::Free(dma_buffer);
                       if (cb) cb(cb_arg, 0, value_len);
                   });
    return;
}

void Engine::DeleteAsync(uint64_t key, KvCallback cb, void* cb_arg) {
    {
        if (state_ != EngineState::kReady) {
            if (cb) cb(cb_arg, static_cast<int>(KvError::kEngineNotReady));
            return;
        }

        // Check if key exists
        MemIndexEntry* existing = mem_index_->Find(key);
        if (!existing) {
            if (cb) cb(cb_arg, static_cast<int>(KvError::kKeyNotFound));
            return;
        }

        // Calculate tombstone size
        size_t header_size = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t);
        size_t entry_size = header_size + sizeof(uint32_t);  // +checksum
        size_t aligned_size = AlignUp(entry_size, kPageSize);

        // Get active file
        FileInfo* file = GetActiveFile();
        if (!file || !file->blob_opened ||
            file->write_offset + aligned_size > config_.data_file_size) {
            if (file) {
                file->state = FileState::kSealed;
            }
            file = AllocateNewFile();
            if (!file) {
                if (cb) cb(cb_arg, static_cast<int>(KvError::kNoSpace));
                return;
            }
            active_file_id_ = file->file_id;
        }

        // Allocate DMA buffer for the tombstone entry
        void* dma_buffer = DmaAllocator::AllocZeroed(aligned_size, kPageSize);
        if (!dma_buffer) {
            if (cb) cb(cb_arg, static_cast<int>(KvError::kInternalError));
            return;
        }

        // Allocate sequence number
        uint32_t seq = mem_index_->AllocateSequence();

        // Build tombstone entry in DMA buffer
        BuildEntryInplace(dma_buffer, key, nullptr, 0, seq, true);

        // Track garbage from the old entry
        uint64_t old_size = existing->page_count * kPageSize;
        uint64_t write_offset = file->write_offset;

        // Update write offset before async operation
        file->write_offset += aligned_size;
        file->size = file->write_offset;
        total_data_bytes_ += aligned_size;

        pending_foreground_count_++;

        // Submit async blob write
        SubmitBlobWrite(file, write_offset, dma_buffer, static_cast<uint32_t>(aligned_size),
                        [this, key, dma_buffer, old_size, cb, cb_arg](int status) {
                            DmaAllocator::Free(dma_buffer);
                            pending_foreground_count_--;

                            if (status == 0) {
                                // Remove from index on successful write
                                mem_index_->Remove(key);
                                total_garbage_bytes_ += old_size;
                            }

                            if (cb)
                                cb(cb_arg, status == 0 ? 0 : static_cast<int>(KvError::kIoError));
                        });
        return;
    }
}

void Engine::Poll() {
    // 1. Process IO completions (this calls spdk_nvme_qpair_process_completions in SPDK mode)
    if (io_submitter_) {
        io_submitter_->ProcessCompletions(32);
    }

    // 2. Process append buffer resets
    if (buffer_manager_) {
        buffer_manager_->CheckPendingResets();
    }

    // 3. Check checkpoint trigger
    CheckCheckpointTrigger();

    // 4. Poll checkpoint progress
    if (checkpoint_manager_ && checkpoint_manager_->IsInProgress()) {
        checkpoint_manager_->Poll();
    }

    // 5. Process compaction (low priority)
    if (compaction_enabled_ && compaction_scheduler_) {
        compaction_scheduler_->SetPendingForegroundCount(GetPendingForegroundCount());
        compaction_scheduler_->Poll();
    }
}

void Engine::StartCheckpoint(KvCallback cb, void* cb_arg) {
    if (!checkpoint_manager_) {
        if (cb) cb(cb_arg, -1);
        return;
    }

    checkpoint_manager_->SetGlobalSequence(mem_index_->GetGlobalSequence());

    checkpoint_manager_->StartCheckpoint([cb, cb_arg](int status) {
        if (cb) cb(cb_arg, status);
    });
}

bool Engine::IsCheckpointInProgress() const {
    return checkpoint_manager_ && checkpoint_manager_->IsInProgress();
}

void Engine::CheckCheckpointTrigger() {
    if (!checkpoint_manager_ || IsCheckpointInProgress()) {
        return;
    }

    // Get current time
    auto now = std::chrono::steady_clock::now();
    uint64_t current_time_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();

    if (checkpoint_trigger_.ShouldCheckpoint(checkpoint_manager_->DirtySegmentCount(),
                                             current_time_ns)) {
        StartCheckpoint(nullptr, nullptr);
        checkpoint_trigger_.Reset(current_time_ns);
    }
}

void Engine::ScheduleCompaction(uint16_t file_id) {
    if (!compaction_scheduler_) return;

    auto it = file_metadata_.find(file_id);
    if (it != file_metadata_.end()) {
        compaction_scheduler_->ScheduleCompaction(&it->second);
    }
}

size_t Engine::GetGarbageRatio() const {
    if (total_data_bytes_ == 0) return 0;
    return (total_garbage_bytes_ * 100) / total_data_bytes_;
}

FileMetadata* Engine::GetFileMetadata(uint16_t file_id) {
    auto it = file_metadata_.find(file_id);
    return (it != file_metadata_.end()) ? &it->second : nullptr;
}

uint64_t Engine::GetEntryCount() const { return mem_index_ ? mem_index_->Size() : 0; }

uint64_t Engine::GetTotalDataBytes() const { return total_data_bytes_; }

double Engine::GetIndexLoadFactor() const { return mem_index_ ? mem_index_->LoadFactor() : 0.0; }

void Engine::AllocateBlobForFile(FileInfo* file, std::function<void(bool success)> callback) {
    if (!file) {
        if (callback) callback(false);
        return;
    }

    auto& env = SpdkEnv::Instance();
    if (!env.IsInitialized()) {
        if (callback) callback(false);
        return;
    }

    pending_blob_ops_++;

    // Allocate blob with the configured data file size
    env.AllocateBlob(config_.data_file_size, [this, file, callback](uint64_t blob_id) {
        pending_blob_ops_--;

        if (blob_id == SPDK_BLOBID_INVALID) {
            if (callback) callback(false);
            return;
        }

        file->blob_id = blob_id;

        // Open the blob after allocation
        OpenBlobForFile(file, callback);
    });
}

void Engine::OpenBlobForFile(FileInfo* file, std::function<void(bool success)> callback) {
    if (!file) {
        if (callback) callback(false);
        return;
    }

    auto& env = SpdkEnv::Instance();
    if (!env.IsInitialized()) {
        if (callback) callback(false);
        return;
    }

    pending_blob_ops_++;

    env.OpenBlob(file->blob_id, [this, file, callback](spdk_blob* blob) {
        pending_blob_ops_--;

        if (!blob) {
            if (callback) callback(false);
            return;
        }

        file->blob = blob;
        file->blob_opened = true;

        // Write file header to blob
        DataFileHeader* header =
                static_cast<DataFileHeader*>(DmaAllocator::AllocZeroed(kPageSize, kPageSize));
        if (!header) {
            if (callback) callback(false);
            return;
        }

        header->magic = kDataFileHeaderMagic;
        header->version = 1;
        header->file_id = file->file_id;
        header->state = FileState::kActive;
        header->create_time = std::chrono::duration_cast<std::chrono::seconds>(
                                      std::chrono::system_clock::now().time_since_epoch())
                                      .count();
        header->checksum = Crc32::Calculate(header, sizeof(*header) - sizeof(header->checksum));

        // Write header to blob
        SubmitBlobWrite(file, 0, header, kPageSize, [header, callback](int status) {
            DmaAllocator::Free(header);
            if (callback) callback(status == 0);
        });
    });
}

void Engine::CloseBlobForFile(FileInfo* file, std::function<void(bool success)> callback) {
    if (!file || !file->blob_opened || !file->blob) {
        if (callback) callback(true);  // Not an error if no blob to close
        return;
    }

    auto& env = SpdkEnv::Instance();
    if (!env.IsInitialized()) {
        if (callback) callback(false);
        return;
    }

    pending_blob_ops_++;

    env.CloseBlob(file->blob, [this, file, callback](int status) {
        pending_blob_ops_--;
        file->blob = nullptr;
        file->blob_opened = false;
        if (callback) callback(status == 0);
    });
}

void Engine::SubmitBlobWrite(FileInfo* file, uint64_t offset, void* data, uint32_t length,
                             std::function<void(int status)> callback) {
    if (!file || !file->blob || !file->blob_opened) {
        if (callback) callback(-1);
        return;
    }

    auto& env = SpdkEnv::Instance();
    if (!env.IsInitialized() || !env.GetBlobStore()) {
        if (callback) callback(-1);
        return;
    }

    auto* channel = io_submitter_->GetChannel();
    if (!channel) {
        if (callback) callback(-1);
        return;
    }

    // Calculate offset and length in io_unit_size
    uint64_t io_unit_size = spdk_bs_get_io_unit_size(env.GetBlobStore());
    uint64_t offset_units = offset / io_unit_size;
    uint64_t length_units = length / io_unit_size;

    // Create completion context
    struct WriteCtx {
        std::function<void(int)> callback;
    };
    auto* ctx = new WriteCtx{std::move(callback)};

    spdk_blob_io_write(
            file->blob, channel, data, offset_units, length_units,
            [](void* arg, int bserrno) {
                auto* ctx = static_cast<WriteCtx*>(arg);
                if (ctx->callback) {
                    ctx->callback(bserrno);
                }
                delete ctx;
            },
            ctx);
}

void Engine::SubmitBlobRead(FileInfo* file, uint64_t offset, void* buffer, uint32_t length,
                            std::function<void(int status)> callback) {
    if (!file || !file->blob || !file->blob_opened) {
        if (callback) callback(-1);
        return;
    }

    auto& env = SpdkEnv::Instance();
    if (!env.IsInitialized() || !env.GetBlobStore()) {
        if (callback) callback(-1);
        return;
    }

    auto* channel = io_submitter_->GetChannel();
    if (!channel) {
        if (callback) callback(-1);
        return;
    }

    // Calculate offset and length in io_unit_size
    uint64_t io_unit_size = spdk_bs_get_io_unit_size(env.GetBlobStore());
    uint64_t offset_units = offset / io_unit_size;
    uint64_t length_units = length / io_unit_size;

    // Create completion context
    struct ReadCtx {
        std::function<void(int)> callback;
    };
    auto* ctx = new ReadCtx{std::move(callback)};

    spdk_blob_io_read(
            file->blob, channel, buffer, offset_units, length_units,
            [](void* arg, int bserrno) {
                auto* ctx = static_cast<ReadCtx*>(arg);
                if (ctx->callback) {
                    ctx->callback(bserrno);
                }
                delete ctx;
            },
            ctx);
}

spdk_blob* Engine::GetBlobForFile(uint16_t file_id) {
    FileInfo* file = GetFile(file_id);
    if (!file || !file->blob_opened) {
        return nullptr;
    }
    return file->blob;
}

KvError Engine::CreateSuperblockBlob() {
    auto& env = SpdkEnv::Instance();
    if (!env.IsInitialized() || !env.GetBlobStore()) {
        return KvError::kInternalError;
    }

    // Step 1: Allocate a blob for the superblock
    struct AllocCtx {
        spdk_blob_id blob_id;
        bool done;
    };
    AllocCtx alloc_ctx{SPDK_BLOBID_INVALID, false};

    env.AllocateBlob(kSuperblockBlobSize, [&alloc_ctx](uint64_t blob_id) {
        alloc_ctx.blob_id = blob_id;
        alloc_ctx.done = true;
    });

    while (!alloc_ctx.done) {
        env.Poll();
    }

    if (alloc_ctx.blob_id == SPDK_BLOBID_INVALID) {
        return KvError::kIoError;
    }

    superblock_blob_id_ = alloc_ctx.blob_id;

    // Step 2: Set as blobstore's super blob
    struct SetSuperCtx {
        int status;
        bool done;
    };
    SetSuperCtx set_ctx{0, false};

    spdk_bs_set_super(env.GetBlobStore(), superblock_blob_id_,
                      [](void* arg, int bserrno) {
                          auto* ctx = static_cast<SetSuperCtx*>(arg);
                          ctx->status = bserrno;
                          ctx->done = true;
                      },
                      &set_ctx);

    while (!set_ctx.done) {
        env.Poll();
    }

    if (set_ctx.status != 0) {
        return KvError::kIoError;
    }

    // Step 3: Open the blob
    struct OpenCtx {
        spdk_blob* blob;
        int status;
        bool done;
    };
    OpenCtx open_ctx{nullptr, 0, false};

    spdk_bs_open_blob(env.GetBlobStore(), superblock_blob_id_,
                      [](void* arg, struct spdk_blob* blob, int bserrno) {
                          auto* ctx = static_cast<OpenCtx*>(arg);
                          ctx->blob = blob;
                          ctx->status = bserrno;
                          ctx->done = true;
                      },
                      &open_ctx);

    while (!open_ctx.done) {
        env.Poll();
    }

    if (open_ctx.status != 0 || !open_ctx.blob) {
        return KvError::kIoError;
    }

    superblock_blob_ = open_ctx.blob;
    return KvError::kSuccess;
}

KvError Engine::WriteSuperblock() {
    auto& env = SpdkEnv::Instance();
    if (!superblock_blob_ || !env.IsInitialized()) {
        return KvError::kInternalError;
    }

    auto* channel = env.GetIoChannel();
    if (!channel) {
        return KvError::kInternalError;
    }

    // Increment sequence and calculate checksum
    superblock_.sequence++;
    superblock_.checksum = Crc32::Calculate(&superblock_, sizeof(Superblock) - sizeof(uint32_t));

    // Allocate DMA buffer and copy superblock
    size_t sb_aligned_size = AlignUp(sizeof(Superblock), kPageSize);
    void* write_buf = DmaAllocator::AllocZeroed(sb_aligned_size, kPageSize);
    if (!write_buf) {
        return KvError::kInternalError;
    }

    std::memcpy(write_buf, &superblock_, sizeof(Superblock));

    uint64_t io_unit_size = spdk_bs_get_io_unit_size(env.GetBlobStore());
    uint64_t length_units = sb_aligned_size / io_unit_size;

    struct WriteCtx {
        int status;
        bool done;
    };

    // Write backup first (design: backup first, then primary)
    WriteCtx backup_ctx{0, false};

    spdk_blob_io_write(superblock_blob_, channel, write_buf,
                       kSuperblockBackupOffset / io_unit_size, length_units,
                       [](void* arg, int bserrno) {
                           auto* ctx = static_cast<WriteCtx*>(arg);
                           ctx->status = bserrno;
                           ctx->done = true;
                       },
                       &backup_ctx);

    while (!backup_ctx.done) {
        env.Poll();
    }

    if (backup_ctx.status != 0) {
        DmaAllocator::Free(write_buf);
        return KvError::kIoError;
    }

    // Write primary
    WriteCtx primary_ctx{0, false};

    spdk_blob_io_write(superblock_blob_, channel, write_buf,
                       kSuperblockPrimaryOffset / io_unit_size, length_units,
                       [](void* arg, int bserrno) {
                           auto* ctx = static_cast<WriteCtx*>(arg);
                           ctx->status = bserrno;
                           ctx->done = true;
                       },
                       &primary_ctx);

    while (!primary_ctx.done) {
        env.Poll();
    }

    DmaAllocator::Free(write_buf);

    if (primary_ctx.status != 0) {
        return KvError::kIoError;
    }

    return KvError::kSuccess;
}

void Engine::UpdateSuperblockFileMapping(FileInfo* file) {
    if (!file) return;

    // Find existing mapping or add new one
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

    KvError err = engine->Create(path ? path : "", create_opts);
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

    KvError err = engine->Open(path ? path : "", open_opts);
    if (err != KvError::kSuccess) {
        delete engine;
        return static_cast<int>(err);
    }

    *handle = engine;
    return 0;
}

int spdk_kv_close(spdk_kv_handle handle) {
    if (!handle) return -1;
    auto* engine = static_cast<Engine*>(handle);
    KvError err = engine->Close();
    delete engine;
    return static_cast<int>(err);
}

int spdk_kv_put(spdk_kv_handle handle, uint64_t key, const void* value, uint32_t value_len) {
    if (!handle) return -1;
    auto* engine = static_cast<Engine*>(handle);
    return static_cast<int>(engine->Put(key, value, value_len));
}

int spdk_kv_get(spdk_kv_handle handle, uint64_t key, void* value_buf, uint32_t buf_len,
                uint32_t* actual_len) {
    if (!handle) return -1;
    auto* engine = static_cast<Engine*>(handle);
    return static_cast<int>(engine->Get(key, value_buf, buf_len, actual_len));
}

int spdk_kv_del(spdk_kv_handle handle, uint64_t key) {
    if (!handle) return -1;
    auto* engine = static_cast<Engine*>(handle);
    return static_cast<int>(engine->Delete(key));
}

void spdk_kv_put_async(spdk_kv_handle handle, uint64_t key, const void* value, uint32_t value_len,
                       spdk_kv_cb cb, void* cb_arg) {
    if (!handle) {
        if (cb) cb(cb_arg, -1);
        return;
    }
    auto* engine = static_cast<Engine*>(handle);
    engine->PutAsync(key, value, value_len, cb, cb_arg);
}

void spdk_kv_get_async(spdk_kv_handle handle, uint64_t key, void* value_buf, uint32_t buf_len,
                       spdk_kv_get_cb cb, void* cb_arg) {
    if (!handle) {
        if (cb) cb(cb_arg, -1, 0);
        return;
    }
    auto* engine = static_cast<Engine*>(handle);
    engine->GetAsync(key, value_buf, buf_len, cb, cb_arg);
}

void spdk_kv_del_async(spdk_kv_handle handle, uint64_t key, spdk_kv_cb cb, void* cb_arg) {
    if (!handle) {
        if (cb) cb(cb_arg, -1);
        return;
    }
    auto* engine = static_cast<Engine*>(handle);
    engine->DeleteAsync(key, cb, cb_arg);
}

void spdk_kv_poll(spdk_kv_handle handle) {
    if (!handle) return;
    auto* engine = static_cast<Engine*>(handle);
    engine->Poll();
}

uint64_t spdk_kv_get_entry_count(spdk_kv_handle handle) {
    if (!handle) return 0;
    auto* engine = static_cast<Engine*>(handle);
    return engine->GetEntryCount();
}

uint64_t spdk_kv_get_total_bytes(spdk_kv_handle handle) {
    if (!handle) return 0;
    auto* engine = static_cast<Engine*>(handle);
    return engine->GetTotalDataBytes();
}

}  // extern "C"

}  // namespace spdk_kv
