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

## 2. 存储架构

### 2.1 整体布局

```
+------------------------------------------------------------------+
|                         NVMe Device                               |
+------------------------------------------------------------------+
| Superblock | Superblock |  MemIndex   |     Data Blob Files       |
|  Primary   |   Backup   | Area (A/B)  |      (N blobs)            |
+------------------------------------------------------------------+
```

### 2.2 区域划分

| 区域 | 用途 | 大小估算 |
|------|------|----------|
| Superblock Primary | 主Superblock | 4KB ~ 4MB |
| Superblock Backup | 备份Superblock (防坏道) | 4KB ~ 4MB |
| MemIndex Area A | 内存索引持久化区域A (分片) | 80GB (80个Segment，每个1GB) |
| MemIndex Area B | 内存索引持久化区域B (分片) | 80GB (80个Segment，每个1GB) |
| Data Blob Files | 实际KV数据存储 | 剩余空间 |

**容量计算说明**:
- 目标记录数: 20亿条
- 索引条目大小: 20字节
- 负载因子: 0.55 (为冲刺100万IOPS降低冲突率)
- 哈希表容量: 20亿 / 0.55 ≈ 36亿槽位
- 总内存需求: 36亿 × 20字节 ≈ 72GB
- Area A + Area B 总预留: 160GB (每个Area 80GB)

**MemIndex Area 分片持久化设计**:

由于 72GB 的连续大内存分配在启动时可能失败，且恢复时需要读取全量数据导致启动时间较长，采用分片设计：

```
MemIndex Area A (80GB) + MemIndex Area B (80GB) = 160GB 总空间
├── Segment 0  (1GB) - blob_id: segment_blobs[0]
├── Segment 1  (1GB) - blob_id: segment_blobs[1]
├── ...
└── Segment 79 (1GB) - blob_id: segment_blobs[79]  (每个 Area 80个 Segment)
```

**优势**:
1. **更可靠的内存分配**: 1GB 的内存块比 80GB 更容易分配成功
2. **更快的启动**: 可以并行加载多个 Segment
3. **增量持久化**: 只持久化变化的 Segment，减少写放大
4. **故障隔离**: 单个 Segment 损坏只影响部分数据

### 2.3 Blob映射设计

由于SPDK blob id的分配不可控，需要设计自定义的file_id到blob_id的映射机制：

```cpp
struct FileMapping {
    uint16_t file_id;       // 自定义文件ID (10bit有效)
    spdk_blob_id blob_id;   // SPDK分配的blob id
    uint64_t size;          // 文件当前大小
    uint64_t write_offset;  // 当前写入位置
    FileState state;        // 文件状态
};

enum class FileState : uint8_t {
    ACTIVE,      // 活跃状态，可读写
    SEALED,      // 已封存，只读
    COMPACTING,  // 正在被Compaction
    DELETED      // 已删除，待回收
};
```

**文件状态转换与读取规则**:
```
ACTIVE ──(写满或主动封存)──> SEALED ──(选中Compaction)──> COMPACTING ──(迁移完成)──> DELETED
   │                           │                            │
   │<──────────────────────────┼────────────────────────────┤
                               │                            │
                           可读状态                       可读状态
```

- **ACTIVE**: 可读写，当前正在追加写入的文件
- **SEALED**: 只读，文件已写满或被主动封存，等待 Compaction
- **COMPACTING**: **仍然可读**，Compaction 正在将有效数据迁移到新文件。在迁移完成并更新所有相关索引前，旧文件必须保持可读状态以响应读请求
- **DELETED**: 不可读，所有有效数据已迁移完成且索引已更新，文件等待物理删除

**重要约束**: Compaction 过程中，只有在以下条件**全部满足**后，才能将源文件从 COMPACTING 转为 DELETED：
1. 所有有效数据已写入目标文件
2. 目标文件数据已持久化 (sync)
3. 所有相关索引条目已更新为指向目标文件
4. 索引更新已持久化 (checkpoint 或 WAL)

**文件ID分配策略**:
- 单个文件大小: 8GB
- 支持最大空间: 8192GB (8TB)
- 需要文件数: 1024个
- file_id位宽: 10bit

---


