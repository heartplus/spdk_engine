// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#pragma once

#include <cstddef>
#include <cstdint>

namespace spdk_kv {

// Forward declarations
class Engine;

// Handle type
using EngineHandle = Engine*;

// Alignment constants
constexpr size_t kPageSize = 4096;                                 // 4KB alignment
constexpr size_t kSectorSize = 512;                                // Sector size
constexpr size_t kDefaultFileSize = 8ULL * 1024 * 1024 * 1024;     // 8GB per file
constexpr size_t kMaxFileCount = 1024;                             // Max number of files
constexpr size_t kMaxCapacity = 8ULL * 1024 * 1024 * 1024 * 1024;  // 8TB

// Magic numbers
constexpr uint32_t kSuperblockMagic = 0x53504B56;      // "SPKV"
constexpr uint32_t kEntryMagic = 0x5350444B;           // "SPDK"
constexpr uint32_t kDataFileHeaderMagic = 0x53504446;  // "SPDF"
constexpr uint32_t kMemIndexMagic = 0x4D494458;        // "MIDX"
constexpr uint32_t kSegmentMagic = 0x53454748;         // "SEGH"

// Superblock layout
constexpr uint64_t kSuperblockPrimaryOffset = 0;
constexpr uint64_t kSuperblockBackupOffset = 4 * 1024 * 1024;  // 4MB
constexpr uint64_t kSuperblockBlobSize = 8 * 1024 * 1024;      // 8MB

// AllocLog constants
constexpr uint32_t kAllocLogMagic = 0x414C4F47;                 // "ALOG"
constexpr uint64_t kAllocLogAreaOffset = 1ULL * 1024 * 1024;    // 1MB offset in superblock blob
constexpr uint32_t kAllocLogCapacity = 64;                       // Max entries in rolling buffer
constexpr uint32_t kAllocLogNearFullThreshold = 48;              // Trigger checkpoint at 75% full
constexpr uint8_t kAllocLogOpAlloc = 1;                          // Allocation operation

// MemIndex configuration
constexpr size_t kMemIndexSegmentSize = 1ULL * 1024 * 1024 * 1024;  // 1GB
constexpr size_t kMemIndexSegmentCount = 80;                        // 80 segments per area
constexpr double kDefaultLoadFactor = 0.55;                         // Load factor for hash table

// AppendBuffer configuration
constexpr size_t kAppendBufferSize = 2 * 1024 * 1024;  // 2MB
constexpr size_t kMinBufferCount = 4;
constexpr size_t kMaxBufferCount = 16;
constexpr size_t kFlushThreshold = kAppendBufferSize - 64 * 1024;  // 2MB - 64KB

// Backpressure thresholds
constexpr size_t kBackpressureHighWater = 12;
constexpr size_t kBackpressureLowWater = 6;

// Error codes
enum class KvError : int {
    kSuccess = 0,
    kKeyNotFound = -1,
    kValueTooLarge = -2,
    kNoSpace = -3,
    kIoError = -4,
    kCorruption = -5,
    kInvalidArgument = -6,
    kEngineNotReady = -7,
    kInternalError = -8,
    kBackpressure = -9,
    kInvalidState = -10,
};

// Engine state
enum class EngineState : uint8_t {
    kUninitialized = 0,
    kOpening,
    kRecovering,
    kReady,
    kClosing,
    kClosed,
    kError
};

// File state
enum class FileState : uint8_t {
    kActive = 0,  // Active, can read/write
    kSealed,      // Sealed, read-only
    kCompacting,  // Being compacted, still readable
    kDeleted      // Deleted, not readable
};

// IO state
enum class IoState : uint8_t { kPending = 0, kBuffered, kWritingData, kCompleted, kFailed };

// Write mode
enum class PutMode : uint8_t {
    LOCAL,      // Local Put, value is a local memory pointer
    RDMA_WRITE  // RDMA Write, value already in DMA Buffer
};

// Entry flags
constexpr uint8_t kFlagDeleted = 0x01;      // Tombstone marker
constexpr uint8_t kFlagCompaction = 0x02;   // Entry created by compaction
constexpr uint8_t kFlagSegmentBuf = 0x04;   // SegmentBuf zero-copy format

// SegmentBuf on-disk layout offsets within the first segment
constexpr size_t kSegmentMetaOffset = 256;   // Metadata starts at byte 256
constexpr size_t kSegmentDataOffset = 512;   // Data resumes at byte 512

// Callback type
using KvCallback = void (*)(void* arg, int status);
using KvGetCallback = void (*)(void* arg, int status, uint32_t actual_len);

// Utility functions
constexpr uint64_t AlignUp(uint64_t value, uint64_t alignment) {
    return (value + alignment - 1) & ~(alignment - 1);
}

constexpr uint64_t AlignDown(uint64_t value, uint64_t alignment) {
    return value & ~(alignment - 1);
}

constexpr bool IsAligned(uint64_t value, uint64_t alignment) {
    return (value & (alignment - 1)) == 0;
}

}  // namespace spdk_kv
