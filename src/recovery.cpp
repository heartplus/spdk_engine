// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#include "engine.h"
#include <cstring>
#include <vector>
#include <algorithm>

#ifdef WITH_SPDK
#include <spdk/blob.h>
#endif

namespace spdk_kv {

// Recovery state machine
enum class RecoveryState {
    INIT,
    LOADING_SUPERBLOCK,
    LOADING_BACKUP_SUPERBLOCK,
    LOADING_MEM_INDEX_A,
    LOADING_MEM_INDEX_B,
    COMPARING_VERSIONS,
    DESERIALIZING,
    SCANNING_DATA_FILES,
    REBUILDING_INCREMENTAL,
    FINALIZING,
    DONE,
    ERROR
};

// Index loader for recovery
class IndexLoader {
public:
    IndexLoader(Engine* engine)
        : engine_(engine)
        , state_(RecoveryState::INIT)
        , current_area_(0)
        , current_read_offset_(0)
        , current_file_idx_(0)
        , current_scan_offset_(0)
        , checkpoint_file_id_(0)
        , checkpoint_page_index_(0)
        , recovered_max_sequence_(0)
        , callback_(nullptr)
        , cb_arg_(nullptr) {
        mem_index_versions_[0] = 0;
        mem_index_versions_[1] = 0;
    }

    // Start async loading
    void start_load(spdk_kv_cb cb, void* cb_arg) {
        callback_ = cb;
        cb_arg_ = cb_arg;
        state_ = RecoveryState::LOADING_SUPERBLOCK;
        step();
    }

    // Poll for recovery progress
    void poll() {
        if (state_ == RecoveryState::DONE || state_ == RecoveryState::ERROR) {
            return;
        }
        step();
    }

    // Check if recovery is complete
    bool is_complete() const {
        return state_ == RecoveryState::DONE || state_ == RecoveryState::ERROR;
    }

    // Check if recovery failed
    bool is_failed() const {
        return state_ == RecoveryState::ERROR;
    }

private:
    void step() {
        switch (state_) {
        case RecoveryState::LOADING_SUPERBLOCK:
            load_superblock();
            break;

        case RecoveryState::LOADING_BACKUP_SUPERBLOCK:
            load_backup_superblock();
            break;

        case RecoveryState::LOADING_MEM_INDEX_A:
            load_mem_index_area(0);
            break;

        case RecoveryState::LOADING_MEM_INDEX_B:
            load_mem_index_area(1);
            break;

        case RecoveryState::COMPARING_VERSIONS:
            compare_and_deserialize();
            break;

        case RecoveryState::DESERIALIZING:
            deserialize_mem_index();
            break;

        case RecoveryState::SCANNING_DATA_FILES:
            start_full_rebuild();
            break;

        case RecoveryState::REBUILDING_INCREMENTAL:
            scan_next_file();
            break;

        case RecoveryState::FINALIZING:
            finalize_recovery();
            break;

        case RecoveryState::DONE:
            complete_recovery(0);
            break;

        case RecoveryState::ERROR:
            complete_recovery(-EIO);
            break;

        default:
            break;
        }
    }

    void load_superblock() {
#ifdef WITH_SPDK
        // Async read primary superblock from superblock blob
        // Allocate DMA buffer for superblock
        void* buf = spdk_dma_zmalloc(sizeof(Superblock), ALIGNMENT, nullptr);
        if (!buf) {
            state_ = RecoveryState::ERROR;
            return;
        }
        superblock_buffer_ = buf;

        // Read superblock blob
        // spdk_blob_io_read(superblock_blob_, channel_, buf,
        //                   0, sizeof(Superblock) / ALIGNMENT,
        //                   on_superblock_read_cb, this);
        // For now, fall through to simulation
        if (validate_superblock()) {
            state_ = RecoveryState::LOADING_MEM_INDEX_A;
        } else {
            state_ = RecoveryState::LOADING_BACKUP_SUPERBLOCK;
        }
#else
        // Simulate successful superblock load
        if (validate_superblock()) {
            state_ = RecoveryState::LOADING_MEM_INDEX_A;
        } else {
            state_ = RecoveryState::LOADING_BACKUP_SUPERBLOCK;
        }
#endif
    }

    void load_backup_superblock() {
#ifdef WITH_SPDK
        // Async read backup superblock
#else
        // Simulate
        if (validate_superblock()) {
            state_ = RecoveryState::LOADING_MEM_INDEX_A;
        } else {
            state_ = RecoveryState::SCANNING_DATA_FILES;
        }
#endif
    }

    bool validate_superblock() {
        // Check magic and checksum
        // In real implementation, would validate from loaded data
        return true;
    }

    void load_mem_index_area(int area) {
        current_area_ = area;
        current_read_offset_ = 0;

#ifdef WITH_SPDK
        // Start async read
        // read_next_chunk();
#else
        // Simulate
        on_area_load_complete();
#endif
    }

    void on_area_load_complete() {
        // Validate and get version
        // In real implementation, would parse header
        mem_index_versions_[current_area_] = current_area_ == 0 ? 1 : 0;

        if (current_area_ == 0) {
            state_ = RecoveryState::LOADING_MEM_INDEX_B;
        } else {
            state_ = RecoveryState::COMPARING_VERSIONS;
        }
    }

    void compare_and_deserialize() {
        // Select area with higher version
        int best_area = (mem_index_versions_[0] >= mem_index_versions_[1]) ? 0 : 1;

        if (mem_index_versions_[best_area] == 0) {
            // Both areas invalid, full rebuild needed
            state_ = RecoveryState::SCANNING_DATA_FILES;
        } else {
            current_area_ = best_area;
            state_ = RecoveryState::DESERIALIZING;
        }
    }

    void deserialize_mem_index() {
        // Load memory index from selected area
        // In real implementation, would deserialize

        state_ = RecoveryState::REBUILDING_INCREMENTAL;
    }

    void start_full_rebuild() {
        // Build list of files to scan
        build_files_to_scan();
        current_file_idx_ = 0;
        state_ = RecoveryState::REBUILDING_INCREMENTAL;
    }

    void build_files_to_scan() {
        // In real implementation, would get file list from superblock
        files_to_scan_.clear();
    }

    void scan_next_file() {
        if (current_file_idx_ >= files_to_scan_.size()) {
            state_ = RecoveryState::FINALIZING;
            return;
        }

        // Scan current file
        scan_file_entries();
    }

    void scan_file_entries() {
#ifdef WITH_SPDK
        // Async read entries
#else
        // Simulate
        current_file_idx_++;
        // Continue to next file or finalize
        if (current_file_idx_ >= files_to_scan_.size()) {
            state_ = RecoveryState::FINALIZING;
        }
#endif
    }

    void parse_and_rebuild_entries(void* buffer, size_t bytes_read) {
        size_t offset = 0;

        while (offset < bytes_read) {
            auto* header = reinterpret_cast<EntryHeader*>(
                static_cast<char*>(buffer) + offset);

            // Validate magic
            if (header->magic != ENTRY_MAGIC) {
                // Skip to next aligned position
                size_t next_aligned = ALIGN_UP(offset + 1, ALIGNMENT);
                if (next_aligned >= bytes_read) break;
                offset = next_aligned;
                continue;
            }

            // Parse entry
            uint64_t key = *reinterpret_cast<uint64_t*>(
                static_cast<char*>(buffer) + offset + sizeof(EntryHeader));
            uint32_t value_len = *reinterpret_cast<uint32_t*>(
                static_cast<char*>(buffer) + offset + sizeof(EntryHeader) + sizeof(uint64_t));

            size_t entry_size = ALIGN_UP(
                sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) + value_len + sizeof(uint32_t),
                ALIGNMENT);

            // Boundary check
            if (offset + entry_size > bytes_read) break;

            // Verify checksum
            uint32_t stored_checksum = *reinterpret_cast<uint32_t*>(
                static_cast<char*>(buffer) + offset + entry_size - sizeof(uint32_t));
            uint32_t computed_checksum = Engine::crc32(
                static_cast<char*>(buffer) + offset, entry_size - sizeof(uint32_t));

            if (stored_checksum != computed_checksum) {
                // CRC mismatch, stop scanning this file
                break;
            }

            // Build index entry
            MemIndexEntry new_entry;
            new_entry.key = key;
            // new_entry.file_id = current_file.file_id;
            // new_entry.offset_index = (current_scan_offset_ + offset) / ALIGNMENT;
            new_entry.page_count = (entry_size + ALIGNMENT - 1) / ALIGNMENT;
            new_entry.deleted = (header->flags & FLAG_DELETED) ? 1 : 0;
            new_entry.sequence = header->sequence;

            uint64_t hash;
            MemIndex::compute_hash(key, &hash, &new_entry.tag);

            // Update max sequence
            if (header->sequence > recovered_max_sequence_) {
                recovered_max_sequence_ = header->sequence;
            }

            // Upsert to index
            if (new_entry.deleted) {
                engine_->mem_index()->remove(key);
            } else {
                engine_->mem_index()->upsert(key, new_entry);
            }

            offset += entry_size;
        }

        current_scan_offset_ += offset;
    }

    void finalize_recovery() {
        // Sync sequence number allocator
        uint32_t checkpoint_seq = 0;  // Would get from superblock
        uint32_t new_global_seq = std::max(checkpoint_seq, recovered_max_sequence_) + 1;
        engine_->mem_index()->set_global_sequence(new_global_seq);

        state_ = RecoveryState::DONE;
    }

    void complete_recovery(int status) {
        if (callback_) {
            callback_(cb_arg_, status);
        }
        callback_ = nullptr;
        cb_arg_ = nullptr;
    }

    // Members
    Engine* engine_;
    RecoveryState state_;

    // Loading state
    int current_area_;
    uint64_t current_read_offset_;
    uint64_t mem_index_versions_[2];

#ifdef WITH_SPDK
    // DMA buffers
    void* superblock_buffer_ = nullptr;
    void* mem_index_buffer_ = nullptr;
    void* scan_buffer_ = nullptr;
#endif

    // Scanning state
    struct FileInfo {
        uint16_t file_id;
        uint64_t size;
    };
    std::vector<FileInfo> files_to_scan_;
    size_t current_file_idx_;
    uint64_t current_scan_offset_;

    // Checkpoint info
    uint16_t checkpoint_file_id_;
    uint64_t checkpoint_page_index_;
    uint32_t recovered_max_sequence_;

    // Callback
    spdk_kv_cb callback_;
    void* cb_arg_;
};

}  // namespace spdk_kv
