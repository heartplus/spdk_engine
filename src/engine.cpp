// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#include "spdk_kv/engine.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>

namespace spdk_kv {

Engine::Engine()
        : state_(EngineState::kUninitialized),
          active_file_id_(0),
          next_file_id_(0),
          total_data_bytes_(0),
          total_garbage_bytes_(0) {
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

    // Clean up resources
    files_.clear();
    mem_index_.reset();
    buffer_manager_.reset();

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

    // Create first data file
    FileInfo* file = AllocateNewFile();
    if (!file) {
        return KvError::kInternalError;
    }
    active_file_id_ = file->file_id;

    // Initialize superblock
    auto now = std::chrono::system_clock::now().time_since_epoch();
    superblock_.magic = kSuperblockMagic;
    superblock_.version = 1;
    superblock_.sequence = 1;
    superblock_.create_time = std::chrono::duration_cast<std::chrono::seconds>(now).count();
    superblock_.last_mount_time = superblock_.create_time;
    superblock_.total_capacity = config_.max_capacity;
    superblock_.data_file_size = config_.data_file_size;
    superblock_.alignment_unit = kPageSize;
    superblock_.active_file_id = active_file_id_;
    superblock_.file_count = 1;

    return KvError::kSuccess;
}

KvError Engine::LoadExisting(const OpenOpts& opts) {
    (void)opts;  // Suppress unused parameter warning (used for future extension)
    // For simulation mode, we don't actually persist data
    // Just initialize as new
    CreateOpts create_opts;
    return InitializeNew(create_opts);
}

KvError Engine::Recover() {
    // No recovery needed in simulation mode
    return KvError::kSuccess;
}

FileInfo* Engine::AllocateNewFile() {
    auto file = std::make_unique<FileInfo>();
    file->file_id = next_file_id_++;
    file->blob_id = file->file_id;  // Use file_id as blob_id in simulation
    file->state = FileState::kActive;
    file->size = 0;
    file->write_offset = sizeof(DataFileHeader);  // Skip header

    // Pre-allocate storage
    file->data.resize(config_.data_file_size);

    // Write file header
    DataFileHeader header;
    std::memset(&header, 0, sizeof(header));
    header.magic = kDataFileHeaderMagic;
    header.version = 1;
    header.file_id = file->file_id;
    header.state = FileState::kActive;
    header.create_time = std::chrono::duration_cast<std::chrono::seconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
    header.checksum = Crc32::Calculate(&header, sizeof(header) - sizeof(header.checksum));
    std::memcpy(file->data.data(), &header, sizeof(header));

    FileInfo* ptr = file.get();
    files_.push_back(std::move(file));
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
    KvError err = Put(key, value, value_len);
    if (cb) {
        cb(cb_arg, static_cast<int>(err));
    }
}

void Engine::GetAsync(uint64_t key, void* value_buf, uint32_t buf_len, KvGetCallback cb,
                      void* cb_arg) {
    uint32_t actual_len = 0;
    KvError err = Get(key, value_buf, buf_len, &actual_len);
    if (cb) {
        cb(cb_arg, static_cast<int>(err), actual_len);
    }
}

void Engine::DeleteAsync(uint64_t key, KvCallback cb, void* cb_arg) {
    KvError err = Delete(key);
    if (cb) {
        cb(cb_arg, static_cast<int>(err));
    }
}

void Engine::Poll() {
    // No-op in simulation mode
    if (buffer_manager_) {
        buffer_manager_->CheckPendingResets();
    }
}

uint64_t Engine::GetEntryCount() const { return mem_index_ ? mem_index_->Size() : 0; }

uint64_t Engine::GetTotalDataBytes() const { return total_data_bytes_; }

double Engine::GetIndexLoadFactor() const { return mem_index_ ? mem_index_->LoadFactor() : 0.0; }

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
