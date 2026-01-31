# SPDK KV Engine 设计文档

## 1. 概述

### 1.1 项目目标

实现一个基于SPDK的高性能KV存储引擎，采用Bitcask模型的设计思想，提供异步IO接口，适用于单线程polling环境。

### 1.2 核心特性

- **Key类型**: `uint64_t`
- **Value**: 符合SPDK内存对齐要求的buffer地址
- **IO模式**: 全异步IO
- **平均Value大小**: > 4KB
- **目标容量**: 单盘8TB，约20亿条记录
- **全路径对齐**: 4KB对齐，支持RDMA Zero-copy
- **性能目标**: 50万+ IOPS

### 1.2.1 执行模型: 单核全 Polling

**设计约束**: 本引擎设计为集成到单线程 polling 环境中，**不考虑线程安全问题**。

## 9. Superblock设计

### 9.1 多副本设计

为防止物理坏道导致无法挂载，Superblock保留两个副本：

```cpp
constexpr uint64_t SUPERBLOCK_PRIMARY_OFFSET = 0;
constexpr uint64_t SUPERBLOCK_BACKUP_OFFSET = 4 * 1024 * 1024;

struct Superblock {
    uint32_t magic;                    // 魔数 "SPKV"
    uint32_t version;
    uint64_t sequence;                 // 更新序列号
    uint64_t create_time;
    uint64_t last_mount_time;

    uint64_t total_capacity;
    uint64_t data_file_size;
    uint32_t alignment_unit;

    spdk_blob_id mem_index_blob_a;
    spdk_blob_id mem_index_blob_b;
    uint64_t mem_index_size;
    uint8_t active_mem_index_area;

    uint16_t active_file_id;
    uint16_t file_count;
    FileMapping file_mappings[1024];

    // Checkpoint 相关 (优化: 使用 page index 而非字节偏移)
    uint64_t checkpoint_sequence;      // Checkpoint 的版本序列号
    uint32_t checkpoint_global_seq;    // Checkpoint 时的全局写入序列号 (用于恢复时正确分配新序列号)
    uint16_t checkpoint_file_id;       // Checkpoint 所在的文件 ID
    uint64_t checkpoint_page_index;    // Checkpoint 位置 (以 4KB page 为单位，直接用于 SPDK 读取)

    // 活跃 Append Buffer 位置记录 (用于恢复时确定扫描范围)
    struct ActiveBufferPos {
        uint16_t file_id;              // 文件 ID
        uint64_t page_index;           // 当前写入位置 (以 page 为单位)
    } active_buffer_positions[16];     // 支持最多 16 个活跃 Buffer (根据 MAX_BUFFER_COUNT 配置)
    uint8_t  active_buffer_count;      // 当前活跃 Buffer 数量

    uint64_t total_entries;
    uint64_t total_data_bytes;
    uint64_t total_garbage_bytes;

    uint32_t checksum;
};
```

### 9.2 Superblock更新策略

```cpp
void update_superblock() {
    superblock_.sequence++;
    superblock_.checksum = calculate_crc32(&superblock_,
                                           sizeof(Superblock) - sizeof(uint32_t));
    // 1. 先写入Backup
    write_superblock_sync(SUPERBLOCK_BACKUP_OFFSET);
    // 2. 再写入Primary
    write_superblock_sync(SUPERBLOCK_PRIMARY_OFFSET);
}
```

---

