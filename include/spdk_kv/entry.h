// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <cstdint>
#include <cstring>

#include "spdk_kv/types.h"

namespace spdk_kv {

// Entry header structure (16 bytes)
struct EntryHeader {
    uint32_t magic;     // Magic number for validation
    uint16_t version;   // Version number
    uint8_t flags;      // Flags (bit0: deleted, bit1: compaction)
    uint8_t reserved;   // Reserved
    uint32_t sequence;  // Write sequence number (persisted)
    uint32_t padding;   // Padding to 16 bytes

    bool is_deleted() const { return (flags & kFlagDeleted) != 0; }
    bool is_compaction() const { return (flags & kFlagCompaction) != 0; }
    bool is_valid() const { return magic == kEntryMagic; }
};

static_assert(sizeof(EntryHeader) == 16, "EntryHeader must be 16 bytes");

// Data file header structure (4KB)
struct DataFileHeader {
    uint32_t magic;         // Magic number 0x53504446 ("SPDF")
    uint32_t version;       // Version number
    uint64_t create_time;   // Creation timestamp
    uint64_t sealed_time;   // Sealed timestamp (0 if not sealed)
    uint64_t entry_count;   // Number of entries
    uint64_t valid_bytes;   // Valid data bytes
    uint64_t total_bytes;   // Total data bytes
    uint16_t file_id;       // File ID
    FileState state;        // File state
    uint8_t reserved;       // Reserved
    uint32_t checksum;      // Header checksum
    uint8_t padding[4040];  // Padding to 4KB (4096 - 56 = 4040)

    bool is_valid() const { return magic == kDataFileHeaderMagic; }
};

static_assert(sizeof(DataFileHeader) == 4096, "DataFileHeader must be 4KB");

// Memory index entry structure (20 bytes)
// Use packed attribute to ensure exact size
struct __attribute__((packed)) MemIndexEntry {
    uint64_t key;  // 8 bytes: user key

    // Bit field packing (4 bytes)
    uint32_t file_id : 10;       // File ID (supports 1024 files)
    uint32_t offset_index : 21;  // Offset index (in 4KB units, supports 8GB)
    uint32_t deleted : 1;        // Delete marker

    // Sequence number (4 bytes) - full 32-bit
    uint32_t sequence;  // Write sequence number

    // page_count and tag (4 bytes)
    uint16_t page_count;  // Number of 4KB pages
    uint8_t tag;          // Hash fingerprint (for fast filtering)
    uint8_t reserved;     // Reserved

    bool is_deleted() const { return deleted != 0; }
    bool is_empty() const { return key == 0 && sequence == 0; }
};

static_assert(sizeof(MemIndexEntry) == 20, "MemIndexEntry must be 20 bytes");

// Segment header for MemIndex persistence
struct SegmentHeader {
    uint32_t magic;           // 0x53454748 ("SEGH")
    uint32_t segment_id;      // Segment number (0-79)
    uint64_t version;         // Segment version
    uint64_t entry_count;     // Valid entry count in this segment
    uint64_t dirty_sequence;  // Last change global sequence
    uint32_t checksum;        // Segment data checksum
    uint8_t padding[28];      // Padding to 64 bytes
};

static_assert(sizeof(SegmentHeader) == 64, "SegmentHeader must be 64 bytes");

// AllocLog entry structure (64 bytes)
// Tracks blob allocations between checkpoints for crash recovery
struct AllocLogEntry {
    uint32_t magic;        // kAllocLogMagic
    uint32_t sequence;     // Monotonic sequence number
    uint64_t blob_id;      // SPDK blob ID
    uint16_t file_id;      // Custom file ID
    uint8_t op;            // Operation type (kAllocLogOpAlloc)
    uint8_t reserved1;
    uint32_t reserved2;
    uint64_t create_time;  // Creation timestamp
    uint64_t file_size;    // Expected file size
    uint8_t padding[20];   // Pad to 60 bytes before checksum
    uint32_t checksum;     // CRC32 of first 60 bytes

    bool is_valid_magic() const { return magic == kAllocLogMagic; }
};

static_assert(sizeof(AllocLogEntry) == 64, "AllocLogEntry must be 64 bytes");

// File mapping structure
struct FileMapping {
    uint16_t file_id;       // Custom file ID (10-bit effective)
    uint64_t blob_id;       // SPDK blob ID
    uint64_t size;          // Current file size
    uint64_t write_offset;  // Current write position
    FileState state;        // File state

    bool is_writable() const { return state == FileState::kActive; }
    bool is_readable() const {
        return state == FileState::kActive || state == FileState::kSealed ||
               state == FileState::kCompacting;
    }
};

// Active buffer position for checkpoint
struct ActiveBufferPos {
    uint16_t file_id;     // File ID
    uint64_t page_index;  // Current write position (in pages)
};

// Superblock structure
struct Superblock {
    uint32_t magic;            // Magic "SPKV"
    uint32_t version;          // Version
    uint64_t sequence;         // Update sequence number
    uint64_t create_time;      // Creation time
    uint64_t last_mount_time;  // Last mount time

    uint64_t total_capacity;  // Total capacity
    uint64_t data_file_size;  // Data file size
    uint32_t alignment_unit;  // Alignment unit

    uint64_t mem_index_blob_a;      // MemIndex Area A blob ID
    uint64_t mem_index_blob_b;      // MemIndex Area B blob ID
    uint64_t mem_index_size;        // MemIndex size
    uint8_t active_mem_index_area;  // Active area (0=A, 1=B)

    uint16_t active_file_id;  // Current active file ID
    uint16_t file_count;      // Number of files
    FileMapping file_mappings[kMaxFileCount];

    // Checkpoint info
    uint64_t checkpoint_sequence;    // Checkpoint version sequence
    uint32_t checkpoint_global_seq;  // Global write sequence at checkpoint
    uint16_t checkpoint_file_id;     // Checkpoint file ID
    uint64_t checkpoint_page_index;  // Checkpoint position (in pages)

    // Active append buffer positions
    ActiveBufferPos active_buffer_positions[kMaxBufferCount];
    uint8_t active_buffer_count;

    uint64_t total_entries;        // Total entry count
    uint64_t total_data_bytes;     // Total data bytes
    uint64_t total_garbage_bytes;  // Total garbage bytes

    // AllocLog tracking (rolling buffer in superblock blob at kAllocLogAreaOffset)
    uint32_t alloc_log_head;      // Head index (oldest valid entry)
    uint32_t alloc_log_tail;      // Tail index (next write position)
    uint32_t alloc_log_sequence;  // Last allocated sequence number

    uint32_t checksum;  // Checksum

    bool is_valid() const { return magic == kSuperblockMagic; }
};

// Pending write structure
struct PendingWrite {
    uint64_t key;
    uint32_t sequence;       // Write sequence number
    uint32_t buffer_offset;  // Offset in AppendBuffer
    uint32_t aligned_size;   // Aligned size
    uint8_t flags;           // Flags (bit0: is_compaction)
    KvCallback callback;
    void* cb_arg;

    // Old position info (only valid for compaction)
    uint16_t old_file_id;
    uint32_t old_offset_index;
    MemIndexEntry new_entry;

    bool is_compaction() const { return (flags & 0x01) != 0; }
};

// AppendSlot: protocol-layer abstraction for RDMA two-phase commit
struct AppendSlot {
    void* addr;        // Points to address inside AppendBuffer
    uint32_t len;      // Total reserved length (aligned)
    uint32_t epoch;    // Buffer epoch (detects buffer reuse)
    uint32_t slot_id;  // Unique slot identifier

    enum class State : uint8_t {
        ALLOCATED,      // Allocated, waiting for RDMA WRITE
        RDMA_COMPLETE,  // RDMA WRITE complete, waiting for COMMIT
        COMMITTED,      // Committed, waiting for flush
        FLUSHED         // Flushed to disk
    };
    State state;
};

// RdmaSlot: RDMA protocol wrapper returned to client
struct RdmaSlot {
    void* buffer;           // Slot start address (= AppendSlot.addr)
    uint32_t value_offset;  // Offset where value data should be written (= header_size)
    uint32_t max_value_len; // Maximum writable value length
    uint64_t rkey;          // RDMA remote key
    uint32_t slot_id;       // Slot ID for subsequent COMMIT call
    uint32_t epoch;         // Buffer epoch (for validation)
};

// BufferedPendingWrite: per-entry tracking within an AppendBuffer IO
struct BufferedPendingWrite {
    uint64_t key;
    uint32_t sequence;
    uint32_t buffer_offset;
    uint32_t aligned_size;
    uint16_t page_count;
    uint8_t tag;
    uint8_t flags;
    KvCallback callback;
    void* cb_arg;
    uint16_t old_file_id;
    uint32_t old_offset_index;
    uint64_t old_garbage_size;
};

// WaitQueueEntry: backpressure deferred writes
struct WaitQueueEntry {
    uint64_t key;
    void* dma_buffer;      // Copied at enqueue time
    uint32_t value_len;
    uint32_t aligned_size;
    uint32_t sequence;
    uint16_t page_count;
    uint8_t tag;
    KvCallback callback;
    void* cb_arg;
    bool is_delete;
    uint64_t old_garbage_size;
};

// Forward declaration
class Engine;
class AppendBuffer;

// BufferIoContext: completion context for buffer IO
struct BufferIoContext {
    Engine* engine;
    AppendBuffer* buffer;
    uint16_t file_id;
    uint32_t base_page_offset;
};

// Serialized MemIndex header
struct SerializedMemIndexHeader {
    uint32_t magic;            // 0x4D494458 ("MIDX")
    uint32_t version;          // Version
    uint64_t entry_count;      // Valid entry count
    uint64_t capacity;         // Hash table capacity
    uint64_t global_sequence;  // Global sequence number
    uint32_t checksum;         // Data checksum
    uint8_t padding[28];       // Padding to 64 bytes
};

static_assert(sizeof(SerializedMemIndexHeader) == 64, "SerializedMemIndexHeader must be 64 bytes");

}  // namespace spdk_kv
