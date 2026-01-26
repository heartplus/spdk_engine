// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#pragma once

#include <cstdint>
#include <cstddef>

namespace spdk_kv {

// Forward declarations
struct Engine;
struct AppendBuffer;
struct MemIndex;

// Handle type for engine instance
using spdk_kv_handle = Engine*;

// Callback function type
// @param cb_arg: User-provided callback argument
// @param status: 0 on success, negative error code on failure
using spdk_kv_cb = void (*)(void* cb_arg, int status);

// RDMA callback with actual length
using spdk_kv_rdma_cb = void (*)(void* cb_arg, int status, uint32_t actual_len);

// Magic numbers
constexpr uint32_t SUPERBLOCK_MAGIC = 0x53504B56;  // "SPKV"
constexpr uint32_t ENTRY_MAGIC = 0x5350444B;       // "SPDK"
constexpr uint32_t DATA_FILE_MAGIC = 0x53504446;   // "SPDF"
constexpr uint32_t MEM_INDEX_MAGIC = 0x4D494458;   // "MIDX"
constexpr uint32_t SEGMENT_MAGIC = 0x53454748;     // "SEGH"

// Alignment
constexpr size_t ALIGNMENT = 4096;  // 4KB alignment

// File state
enum class FileState : uint8_t {
    ACTIVE = 0,     // Active, can read/write
    SEALED = 1,     // Sealed, read only
    COMPACTING = 2, // Being compacted, still readable
    DELETED = 3     // Deleted, waiting for reclaim
};

// Engine state
enum class EngineState : uint8_t {
    UNINITIALIZED = 0,
    OPENING = 1,
    RECOVERING = 2,
    READY = 3,
    CLOSING = 4,
    CLOSED = 5,
    ERROR = 6
};

// IO state
enum class IoState : uint8_t {
    PENDING = 0,
    BUFFERED = 1,
    WRITING_DATA = 2,
    COMPLETED = 3,
    FAILED = 4
};

// Entry flags
constexpr uint8_t FLAG_DELETED = 0x01;
constexpr uint8_t FLAG_COMPACTION = 0x02;

// Data entry header (16 bytes)
struct EntryHeader {
    uint32_t magic;      // Magic number for validation
    uint16_t version;    // Version number
    uint8_t flags;       // Flags (bit0: deleted, bit1: compaction)
    uint8_t reserved;    // Reserved
    uint32_t sequence;   // Write sequence number (persisted)
    uint32_t padding;    // Padding to 16 bytes
};
static_assert(sizeof(EntryHeader) == 16, "EntryHeader must be 16 bytes");

// Data file header (4KB)
struct DataFileHeader {
    uint32_t magic;           // Magic number 0x53504446 ("SPDF") - 4 bytes
    uint32_t version;         // Version number - 4 bytes
    uint16_t file_id;         // File ID - 2 bytes
    FileState state;          // File state - 1 byte
    uint8_t reserved;         // Reserved - 1 byte
    uint32_t padding1;        // Padding for alignment - 4 bytes (total 16)
    uint64_t create_time;     // Creation timestamp - 8 bytes
    uint64_t sealed_time;     // Sealed timestamp (0 if not sealed) - 8 bytes
    uint64_t entry_count;     // Number of entries - 8 bytes
    uint64_t valid_bytes;     // Valid data bytes - 8 bytes
    uint64_t total_bytes;     // Total data bytes - 8 bytes
    uint32_t checksum;        // Header checksum - 4 bytes
    uint8_t padding[4036];    // Padding to 4KB (4096 - 60 = 4036)
};
static_assert(sizeof(DataFileHeader) == 4096, "DataFileHeader must be 4KB");

// Memory index entry (20 bytes) - packed to ensure exact size
#pragma pack(push, 1)
struct MemIndexEntry {
    uint64_t key;                    // 8 bytes: User key

    // Bit fields (4 bytes)
    uint32_t file_id      : 10;      // File ID (supports 1024 files)
    uint32_t offset_index : 21;      // Offset index (in 4KB units, supports 8GB)
    uint32_t deleted      : 1;       // Delete flag

    // Sequence number (4 bytes)
    uint32_t sequence;               // Write sequence (32bit)

    // Page count and tag (4 bytes)
    uint16_t page_count;             // Value pages (4KB units)
    uint8_t tag;                     // Hash tag for fast filtering
    uint8_t reserved;                // Reserved
};
#pragma pack(pop)
static_assert(sizeof(MemIndexEntry) == 20, "MemIndexEntry must be 20 bytes");

// File mapping
struct FileMapping {
    uint16_t file_id;       // Custom file ID (10bit effective)
    uint64_t blob_id;       // SPDK blob ID
    uint64_t size;          // Current file size
    uint64_t write_offset;  // Current write position
    FileState state;        // File state
    uint8_t padding[5];     // Padding
};

// Active buffer position (for recovery)
struct ActiveBufferPos {
    uint16_t file_id;       // File ID
    uint64_t page_index;    // Current write position (in pages)
};

// Segment header for MemIndex
struct SegmentHeader {
    uint32_t magic;              // 0x53454748 ("SEGH")
    uint32_t segment_id;         // Segment number (0-79)
    uint64_t version;            // Segment version
    uint64_t entry_count;        // Valid entry count in this segment
    uint64_t dirty_sequence;     // Last dirty global sequence
    uint32_t checksum;           // Segment data checksum
    uint8_t padding[28];         // Padding to 64 bytes
};
static_assert(sizeof(SegmentHeader) == 64, "SegmentHeader must be 64 bytes");

// Superblock structure
struct Superblock {
    uint32_t magic;                    // Magic "SPKV"
    uint32_t version;                  // Version
    uint64_t sequence;                 // Update sequence
    uint64_t create_time;              // Creation time
    uint64_t last_mount_time;          // Last mount time

    uint64_t total_capacity;           // Total capacity
    uint64_t data_file_size;           // Data file size
    uint32_t alignment_unit;           // Alignment unit

    uint64_t mem_index_blob_a;         // MemIndex blob A
    uint64_t mem_index_blob_b;         // MemIndex blob B
    uint64_t mem_index_size;           // MemIndex size
    uint8_t active_mem_index_area;     // Active MemIndex area (0=A, 1=B)

    uint16_t active_file_id;           // Active file ID
    uint16_t file_count;               // File count
    FileMapping file_mappings[1024];   // File mappings

    // Checkpoint info
    uint64_t checkpoint_sequence;      // Checkpoint version sequence
    uint32_t checkpoint_global_seq;    // Global sequence at checkpoint
    uint16_t checkpoint_file_id;       // Checkpoint file ID
    uint64_t checkpoint_page_index;    // Checkpoint position (in pages)

    // Active buffer positions
    ActiveBufferPos active_buffer_positions[16];
    uint8_t active_buffer_count;       // Active buffer count

    uint64_t total_entries;            // Total entries
    uint64_t total_data_bytes;         // Total data bytes
    uint64_t total_garbage_bytes;      // Total garbage bytes

    uint32_t checksum;                 // Checksum
};

// Pending write context
struct PendingWrite {
    uint64_t key;
    uint32_t sequence;           // Write sequence
    uint32_t buffer_offset;      // Offset in AppendBuffer
    uint32_t aligned_size;       // Aligned size
    uint8_t flags;               // Flags: bit0 = is_compaction
    spdk_kv_cb callback;
    void* cb_arg;

    // Old position info (only valid for compaction)
    uint16_t old_file_id;
    uint32_t old_offset_index;
    MemIndexEntry new_entry;

    bool is_compaction() const { return flags & 0x01; }
};

// Utility macros
#define ALIGN_UP(x, align) (((x) + (align) - 1) & ~((align) - 1))
#define ALIGN_DOWN(x, align) ((x) & ~((align) - 1))

}  // namespace spdk_kv
