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

## 3. 数据格式

### 3.1 全路径4KB对齐设计

为支持RDMA Zero-copy和NVMe高效IO，全路径采用4KB对齐：

```
Disk -> SPDK Buffer -> RDMA NIC -> Client
         (4KB对齐)     (MR已注册)
```

**对齐规则**:
- 对齐单位: 4KB (4096 bytes)
- 所有Entry起始位置必须4KB对齐
- 实际占用空间 = ALIGN_UP(entry_size, 4096)

### 3.2 数据文件头 (DataFileHeader)

每个数据文件开头包含一个文件头，用于恢复时判断文件状态：

```cpp
struct DataFileHeader {
    uint32_t magic;              // 魔数 0x53504446 ("SPDF")
    uint32_t version;            // 版本号
    uint16_t file_id;            // 文件ID
    FileState state;             // 文件状态
    uint8_t reserved;
    uint64_t create_time;        // 创建时间戳
    uint64_t sealed_time;        // 封存时间戳 (0表示未封存)
    uint64_t entry_count;        // 记录数量
    uint64_t valid_bytes;        // 有效数据字节数
    uint64_t total_bytes;        // 总数据字节数
    uint32_t checksum;           // Header校验和
    uint8_t padding[4056];       // 填充到4KB
};  // 总计: 4KB
```

### 3.3 数据记录格式 (Data Entry)

每条写入的数据在Data Blob中按如下格式存储：

```
+--------+--------+----------+------------+---------+----------+
| Header | Key    | ValueLen | Value      | Padding | Checksum |
| 8B     | 8B     | 4B       | Variable   | Var     | 4B       |
+--------+--------+----------+------------+---------+----------+
|<------ 对齐到4KB边界 -------------------------------------->|
```

**Header结构 (16 bytes)**:
```cpp
struct EntryHeader {
    uint32_t magic;         // 魔数，用于校验 0x5350444B ("SPDK")
    uint16_t version;       // 版本号
    uint8_t  flags;         // 标志位 (bit0: 删除标记, bit1: compaction产生)
    uint8_t  reserved;      // 保留
    uint32_t sequence;      // 写入序列号 (持久化，用于恢复时保持顺序)
    uint32_t padding;       // 填充到16字节对齐
};

// flags定义
constexpr uint8_t FLAG_DELETED    = 0x01;  // 删除标记
constexpr uint8_t FLAG_COMPACTION = 0x02;  // Compaction产生的记录
```

**设计说明**:
- `sequence` 字段持久化写入序列号，解决重启后索引恢复顺序问题
- 恢复时使用持久化的 sequence 而非重新分配，保证同一 key 的多次写入顺序正确

### 3.4 删除记录格式 (Tombstone)

删除操作写入一条特殊记录，flags的bit0设置为1：

```
+--------+--------+----------+
| Header | Key    | Checksum |
| 8B     | 8B     | 4B       |
+--------+--------+----------+
|<-- 对齐到4KB边界 -------->|
```

---


