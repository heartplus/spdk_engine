// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#include "spdk_kv/alloc_log.h"

#include <chrono>
#include <cstring>

#include "spdk_kv/crc32.h"
#include "spdk_kv/spdk_env.h"

namespace spdk_kv {

AllocLogManager::AllocLogManager()
        : superblock_blob_(nullptr),
          channel_(nullptr),
          blobstore_(nullptr),
          head_(0),
          tail_(0),
          sequence_(0),
          page_buf_(nullptr),
          initialized_(false) {}

AllocLogManager::~AllocLogManager() {
    if (page_buf_) {
        DmaAllocator::free(page_buf_);
        page_buf_ = nullptr;
    }
}

void AllocLogManager::initialize(spdk_blob* superblock_blob, spdk_io_channel* channel,
                                 spdk_blob_store* blobstore, uint32_t head, uint32_t tail,
                                 uint32_t sequence) {
    superblock_blob_ = superblock_blob;
    channel_ = channel;
    blobstore_ = blobstore;
    head_ = head;
    tail_ = tail;
    sequence_ = sequence;

    // Allocate DMA-aligned buffer for the AllocLog page (4KB)
    page_buf_ = DmaAllocator::alloc_zeroed(kPageSize, kPageSize);
    initialized_ = (page_buf_ != nullptr);
}

void AllocLogManager::write_entry(uint64_t blob_id, uint16_t file_id, uint64_t file_size,
                                 std::function<void(int status)> callback) {
    if (!initialized_ || is_full()) {
        if (callback) {
            callback(-1);
        }
        return;
    }

    // Build the AllocLog entry
    AllocLogEntry entry;
    std::memset(&entry, 0, sizeof(entry));
    entry.magic = kAllocLogMagic;
    entry.sequence = ++sequence_;
    entry.blob_id = blob_id;
    entry.file_id = file_id;
    entry.op = kAllocLogOpAlloc;
    auto now = std::chrono::system_clock::now().time_since_epoch();
    entry.create_time = std::chrono::duration_cast<std::chrono::seconds>(now).count();
    entry.file_size = file_size;
    entry.checksum = Crc32::calculate(&entry, sizeof(AllocLogEntry) - sizeof(uint32_t));

    // write to in-memory page buffer at the tail position
    uint32_t slot = tail_ % kAllocLogCapacity;
    auto* entries = reinterpret_cast<AllocLogEntry*>(page_buf_);
    entries[slot] = entry;

    // Advance tail
    tail_++;

    // write the entire page to superblock blob at kAllocLogAreaOffset
    if (!superblock_blob_ || !channel_ || !blobstore_) {
        // No SPDK resources: in-memory only (for testing)
        if (callback) {
            callback(0);
        }
        return;
    }

    uint64_t io_unit_size = spdk_bs_get_io_unit_size(blobstore_);
    uint64_t offset_units = kAllocLogAreaOffset / io_unit_size;
    uint64_t length_units = kPageSize / io_unit_size;

    auto* ctx = write_ctx_pool_.alloc(std::move(callback), this);

    spdk_blob_io_write(
            superblock_blob_, channel_, page_buf_, offset_units, length_units,
            [](void* arg, int bserrno) {
                auto* ctx = static_cast<AllocLogWriteCtx*>(arg);
                auto cb = std::move(ctx->callback);
                ctx->mgr->write_ctx_pool_.free(ctx);
                if (cb) {
                    cb(bserrno);
                }
            },
            ctx);
}

void AllocLogManager::load_entries(
        uint32_t checkpoint_sequence,
        std::function<void(int status, const std::vector<AllocLogEntry>& entries)> callback) {
    if (!initialized_) {
        std::vector<AllocLogEntry> empty;
        if (callback) {
            callback(-1, empty);
        }
        return;
    }

    if (!superblock_blob_ || !channel_ || !blobstore_) {
        // No SPDK resources: parse from in-memory buffer
        std::vector<AllocLogEntry> result;
        auto* page_entries = reinterpret_cast<AllocLogEntry*>(page_buf_);
        uint32_t last_seq = checkpoint_sequence;

        for (uint32_t i = 0; i < kAllocLogCapacity; i++) {
            uint32_t slot = (head_ + i) % kAllocLogCapacity;
            const auto& e = page_entries[slot];

            if (!e.is_valid_magic()) {
                break;
            }

            uint32_t computed =
                    Crc32::calculate(&e, sizeof(AllocLogEntry) - sizeof(uint32_t));
            if (computed != e.checksum) {
                break;
            }

            if (e.sequence <= last_seq) {
                break;
            }

            last_seq = e.sequence;
            result.push_back(e);
        }

        // Update tail based on actual entries found
        tail_ = head_ + static_cast<uint32_t>(result.size());
        if (!result.empty()) {
            sequence_ = result.back().sequence;
        }

        if (callback) {
            callback(0, result);
        }
        return;
    }

    // Read the AllocLog page from superblock blob
    uint64_t io_unit_size = spdk_bs_get_io_unit_size(blobstore_);
    uint64_t offset_units = kAllocLogAreaOffset / io_unit_size;
    uint64_t length_units = kPageSize / io_unit_size;

    auto* ctx = load_entries_ctx_pool_.alloc(this, checkpoint_sequence, std::move(callback));

    spdk_blob_io_read(superblock_blob_, channel_, page_buf_, offset_units, length_units,
                      on_alloc_log_page_read, ctx);
}

void AllocLogManager::on_alloc_log_page_read(void* arg, int bserrno) {
    auto* ctx = static_cast<LoadEntriesReadCtx*>(arg);
    std::vector<AllocLogEntry> result;

    if (bserrno != 0) {
        auto cb = std::move(ctx->callback);
        ctx->mgr->load_entries_ctx_pool_.free(ctx);
        if (cb) {
            cb(bserrno, result);
        }
        return;
    }

    // Parse valid entries from the page
    auto* entries = reinterpret_cast<AllocLogEntry*>(ctx->mgr->page_buf_);
    uint32_t head = ctx->mgr->head_;
    uint32_t last_seq = ctx->checkpoint_seq;

    for (uint32_t i = 0; i < kAllocLogCapacity; i++) {
        uint32_t slot = (head + i) % kAllocLogCapacity;
        const auto& e = entries[slot];

        if (!e.is_valid_magic()) {
            break;
        }

        uint32_t computed =
                Crc32::calculate(&e, sizeof(AllocLogEntry) - sizeof(uint32_t));
        if (computed != e.checksum) {
            break;
        }

        if (e.sequence <= last_seq) {
            break;
        }

        last_seq = e.sequence;
        result.push_back(e);
    }

    // Update tail based on recovered entries
    ctx->mgr->tail_ = head + static_cast<uint32_t>(result.size());
    if (!result.empty()) {
        ctx->mgr->sequence_ = result.back().sequence;
    }

    auto cb = std::move(ctx->callback);
    ctx->mgr->load_entries_ctx_pool_.free(ctx);
    if (cb) {
        cb(0, result);
    }
}

void AllocLogManager::reclaim() {
    // Advance head to tail, logically emptying the log.
    // Physical page data remains but will be overwritten by future entries.
    // The head/tail values are persisted to superblock during checkpoint.
    head_ = tail_;
}

bool AllocLogManager::is_near_full() const {
    return used_count() >= kAllocLogNearFullThreshold;
}

bool AllocLogManager::is_full() const {
    return used_count() >= kAllocLogCapacity;
}

}  // namespace spdk_kv
