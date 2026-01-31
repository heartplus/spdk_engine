// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#include "spdk_kv/recovery.h"

#include <algorithm>
#include <cstring>

#include "spdk_kv/crc32.h"

namespace spdk_kv {

IndexLoader::IndexLoader(MemIndex* mem_index)
        : mem_index_(mem_index),
          state_(State::kInit),
          last_error_(KvError::kSuccess),
          selected_area_(-1),
          checkpoint_file_id_(0),
          checkpoint_page_index_(0),
          recovered_max_sequence_(0),
          current_scan_idx_(0),
          current_scan_offset_(0) {
    mem_index_versions_[0] = 0;
    mem_index_versions_[1] = 0;
    std::memset(&superblock_, 0, sizeof(superblock_));

    // Allocate buffers for simulation
    superblock_buffer_.resize(sizeof(Superblock));
    mem_index_buffer_.resize(64 * 1024 * 1024);  // 64MB
    scan_buffer_.resize(1024 * 1024);            // 1MB
}

IndexLoader::~IndexLoader() {}

void IndexLoader::StartRecovery(RecoveryCallback callback) {
    if (state_ != State::kInit) {
        callback(KvError::kInvalidState);
        return;
    }

    callback_ = std::move(callback);
    TransitionTo(State::kLoadingSuperblockPrimary);
    LoadSuperblockPrimary();
}

bool IndexLoader::Poll() {
    // In simulation mode, recovery is synchronous
    // In real SPDK mode, this would check IO completion
    return !IsComplete();
}

void IndexLoader::TransitionTo(State new_state) { state_ = new_state; }

void IndexLoader::LoadSuperblockPrimary() {
    // In simulation mode, superblock is already set via SetSuperblock()
    // In real SPDK mode, this would read from blob
    if (ValidateSuperblock(superblock_)) {
        TransitionTo(State::kLoadingMemIndexA);
        LoadMemIndexArea(0);
    } else {
        // Try backup superblock
        TransitionTo(State::kLoadingSuperblockBackup);
        LoadSuperblockBackup();
    }
}

void IndexLoader::LoadSuperblockBackup() {
    // In simulation mode, no backup superblock
    // In real SPDK mode, this would read from backup blob
    if (ValidateSuperblock(superblock_)) {
        TransitionTo(State::kLoadingMemIndexA);
        LoadMemIndexArea(0);
    } else {
        // Both superblocks invalid, need full rebuild
        last_error_ = KvError::kCorruption;
        TransitionTo(State::kError);
        if (callback_) {
            callback_(last_error_);
        }
    }
}

void IndexLoader::LoadMemIndexArea(int area) {
    // In simulation mode, we skip actual loading
    // Just validate the area

    // In real SPDK mode, we would read and validate header
    // auto* header = reinterpret_cast<SerializedMemIndexHeader*>(mem_index_buffer_.data());
    (void)mem_index_buffer_;  // Suppress unused warning in simulation

    // For simulation, assume the area is valid if superblock points to it
    if (area == 0) {
        mem_index_versions_[0] = superblock_.checkpoint_sequence;
    } else {
        mem_index_versions_[1] = 0;  // Assume B is older
    }

    if (area == 0) {
        TransitionTo(State::kLoadingMemIndexB);
        LoadMemIndexArea(1);
    } else {
        TransitionTo(State::kComparingVersions);
        CompareAndSelectArea();
    }
}

void IndexLoader::CompareAndSelectArea() {
    // Select area with higher version
    selected_area_ = (mem_index_versions_[0] >= mem_index_versions_[1]) ? 0 : 1;

    if (mem_index_versions_[selected_area_] == 0) {
        // No valid MemIndex area, need full rebuild
        TransitionTo(State::kScanningDataFiles);
        StartIncrementalRebuild();
    } else {
        TransitionTo(State::kDeserializing);
        DeserializeMemIndex(selected_area_);
    }
}

void IndexLoader::DeserializeMemIndex(int area) {
    // Check if we can use memory dump load
    if (CanUseMemoryDumpLoad()) {
        LoadAsMemoryDump(area);
    } else {
        LoadByUpsert(area);
    }

    // Continue to incremental rebuild
    TransitionTo(State::kRebuildingIncremental);
    StartIncrementalRebuild();
}

bool IndexLoader::CanUseMemoryDumpLoad() const {
    // In simulation mode, always use upsert for simplicity
    return false;
}

void IndexLoader::LoadAsMemoryDump(int area) {
    (void)area;
    // Memory dump load: directly copy data to MemIndex
    // This requires matching capacity and layout
    RebuildPslArray();
}

void IndexLoader::RebuildPslArray() {
    if (!mem_index_) {
        return;
    }
    mem_index_->RebuildPslArray();
}

void IndexLoader::LoadByUpsert(int area) {
    (void)area;
    // Upsert load: insert entries one by one
    // This is slower but works when capacity changes

    // In simulation mode, mem_index is already populated
    // In real SPDK mode, we would iterate through serialized data
}

void IndexLoader::StartIncrementalRebuild() {
    // Get checkpoint info from superblock
    checkpoint_file_id_ = superblock_.checkpoint_file_id;
    checkpoint_page_index_ = superblock_.checkpoint_page_index;
    recovered_max_sequence_ = superblock_.checkpoint_global_seq;

    // Build scan ranges
    scan_ranges_ = BuildScanRanges();

    if (scan_ranges_.empty()) {
        // No files to scan, finalize
        TransitionTo(State::kFinalizingRecovery);
        FinalizeRecovery();
        return;
    }

    current_scan_idx_ = 0;
    ScanNextFile();
}

std::vector<ScanRange> IndexLoader::BuildScanRanges() {
    std::vector<ScanRange> ranges;

    for (const auto& file : file_data_) {
        ScanRange range;
        range.file_id = file.file_id;

        if (file.file_id == checkpoint_file_id_) {
            // Start from checkpoint position
            range.start_page = checkpoint_page_index_;
        } else if (file.file_id > checkpoint_file_id_) {
            // Files created after checkpoint, scan from beginning
            range.start_page = sizeof(DataFileHeader) / kPageSize;
        } else {
            // Files before checkpoint are already in index
            continue;
        }

        // End at physical file end
        range.end_page = file.size / kPageSize;

        if (range.end_page > range.start_page) {
            ranges.push_back(range);
        }
    }

    // Sort by file_id
    std::sort(ranges.begin(), ranges.end(),
              [](const ScanRange& a, const ScanRange& b) { return a.file_id < b.file_id; });

    return ranges;
}

void IndexLoader::ScanNextFile() {
    if (current_scan_idx_ >= scan_ranges_.size()) {
        // All files scanned
        TransitionTo(State::kFinalizingRecovery);
        FinalizeRecovery();
        return;
    }

    const auto& range = scan_ranges_[current_scan_idx_];
    current_scan_offset_ = range.start_page * kPageSize;

    // Find file data
    const char* data = nullptr;
    size_t size = 0;
    for (const auto& file : file_data_) {
        if (file.file_id == range.file_id) {
            data = file.data;
            size = file.size;
            break;
        }
    }

    if (data && size > current_scan_offset_) {
        // Parse entries from this file
        size_t entries_found =
                ParseAndRebuildEntries(data + current_scan_offset_, size - current_scan_offset_,
                                       range.file_id, current_scan_offset_);
        (void)entries_found;
    }

    // Move to next file
    current_scan_idx_++;
    ScanNextFile();
}

size_t IndexLoader::ParseAndRebuildEntries(const void* buffer, size_t buffer_size, uint16_t file_id,
                                           uint64_t base_offset) {
    const char* ptr = static_cast<const char*>(buffer);
    size_t offset = 0;
    size_t entries_processed = 0;

    while (offset + sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) <= buffer_size) {
        const auto* header = reinterpret_cast<const EntryHeader*>(ptr + offset);

        // Validate magic
        if (header->magic != kEntryMagic) {
            // Invalid magic, try next aligned position
            size_t next_aligned = AlignUp(offset + 1, kPageSize);
            if (next_aligned >= buffer_size) {
                break;
            }
            offset = next_aligned;
            continue;
        }

        // Parse entry
        uint64_t key = *reinterpret_cast<const uint64_t*>(ptr + offset + sizeof(EntryHeader));
        uint32_t value_len = *reinterpret_cast<const uint32_t*>(ptr + offset + sizeof(EntryHeader) +
                                                                sizeof(uint64_t));

        // Calculate entry size
        size_t entry_size = AlignUp(sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) +
                                            value_len + sizeof(uint32_t),
                                    kPageSize);

        // Boundary check
        if (offset + entry_size > buffer_size) {
            break;
        }

        // Validate checksum
        uint32_t stored_checksum =
                *reinterpret_cast<const uint32_t*>(ptr + offset + entry_size - sizeof(uint32_t));
        uint32_t computed_checksum = Crc32::Calculate(ptr + offset, entry_size - sizeof(uint32_t));

        if (stored_checksum != computed_checksum) {
            // CRC mismatch, stop scanning this file
            break;
        }

        // Update index
        MemIndexEntry new_entry;
        new_entry.key = key;
        new_entry.file_id = file_id;
        new_entry.offset_index = static_cast<uint32_t>((base_offset + offset) / kPageSize);
        new_entry.page_count = static_cast<uint16_t>(entry_size / kPageSize);
        new_entry.deleted = (header->flags & kFlagDeleted) ? 1 : 0;
        new_entry.sequence = header->sequence;

        // Compute tag
        uint64_t hash;
        HashUtil::ComputeHash(key, &hash, &new_entry.tag);

        // Update recovered max sequence
        if (header->sequence > recovered_max_sequence_) {
            recovered_max_sequence_ = header->sequence;
        }

        // Upsert to index
        if (new_entry.deleted) {
            mem_index_->Remove(key);
        } else {
            mem_index_->Upsert(key, new_entry);
        }

        entries_processed++;
        offset += entry_size;
    }

    return entries_processed;
}

void IndexLoader::FinalizeRecovery() {
    // Sync global sequence number
    // new_global_seq = max(checkpoint_seq, recovered_max_seq) + 1
    uint32_t checkpoint_seq = superblock_.checkpoint_global_seq;
    uint32_t new_global_seq = std::max(checkpoint_seq, recovered_max_sequence_) + 1;
    mem_index_->SetGlobalSequence(new_global_seq);

    TransitionTo(State::kDone);

    if (callback_) {
        callback_(KvError::kSuccess);
    }
}

bool IndexLoader::ValidateSuperblock(const Superblock& sb) {
    if (sb.magic != kSuperblockMagic) {
        return false;
    }

    // Additional validation can be added here
    // - Check version compatibility
    // - Validate checksum
    // - Verify file mappings

    return true;
}

bool IndexLoader::ValidateChecksum(const void* data, size_t size, uint32_t expected) {
    uint32_t computed = Crc32::Calculate(data, size);
    return computed == expected;
}

}  // namespace spdk_kv
