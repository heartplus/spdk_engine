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

## 10. 状态机设计

### 10.1 Engine状态

```cpp
enum class EngineState {
    UNINITIALIZED,
    OPENING,
    RECOVERING,
    READY,
    CLOSING,
    CLOSED,
    ERROR
};
```

### 10.2 IO操作状态机

```cpp
enum class IoState {
    PENDING,
    BUFFERED,
    WRITING_DATA,
    COMPLETED,
    FAILED
};
```

---

## 11. 错误处理

### 11.1 错误码定义

```cpp
enum class KvError {
    SUCCESS = 0,
    KEY_NOT_FOUND,
    VALUE_TOO_LARGE,
    NO_SPACE,
    IO_ERROR,
    CORRUPTION,
    INVALID_ARGUMENT,
    ENGINE_NOT_READY,
    INTERNAL_ERROR
};
```

---

## 12. 配置参数

```cpp
struct SpkdKvConfig {
    // 容量配置
    uint64_t max_capacity = 8ULL * 1024 * 1024 * 1024 * 1024;  // 8TB
    uint64_t data_file_size = 8ULL * 1024 * 1024 * 1024;       // 8GB

    // 索引配置 (优化后)
    uint64_t max_entries = 2000000000ULL;  // 20亿
    double index_load_factor = 0.55;       // 优化: 从0.7降至0.55，换取更短探测路径
    size_t index_entry_size = 20;          // 优化: 从16字节扩展到20字节 (32bit序列号)

    // IO配置
    uint32_t alignment = 4096;
    uint32_t max_value_size = 256 * 1024 * 1024;  // 256MB (page_count 16bit限制)

    // AppendBuffer配置 (动态调整)
    size_t append_buffer_size = 2 * 1024 * 1024;
    size_t append_buffer_min_count = 4;           // 最小buffer数量
    size_t append_buffer_max_count = 16;          // 最大buffer数量
    size_t backpressure_high_water = 12;          // 触发backpressure
    size_t backpressure_low_water = 6;            // 恢复接受请求
    uint32_t flush_timeout_ms = 10;
    size_t flush_entry_threshold = 128;           // Entry数量触发刷盘
    size_t flush_buffer_threshold = 16;           // 积压buffer数量触发刷盘

    // Compaction配置 (增强)
    double compaction_threshold = 0.5;
    uint32_t compaction_max_iops = 1000;
    uint64_t compaction_max_cycles = 50000;       // per poll cycle
    int compaction_max_retry = 3;                 // IO重试次数
    uint64_t compaction_retry_delay_us = 1000;    // 重试延迟
    double bitmap_creation_threshold = 0.3;       // 垃圾率超过30%时创建详细位图

    // 内存池配置
    size_t read_buffer_count = 256;
    size_t read_buffer_size = 64 * 1024;

    // NUMA配置
    int cpu_core = -1;  // -1表示自动选择

    // 持久化配置 (增量Checkpoint)
    uint32_t checkpoint_interval_sec = 300;
    uint64_t checkpoint_bytes = 10ULL * 1024 * 1024 * 1024;  // 10GB写入触发
    uint32_t checkpoint_dirty_segment_threshold = 8;          // 8个脏Segment触发
    size_t checkpoint_segment_size = 1ULL * 1024 * 1024 * 1024;  // 1GB per segment
    size_t checkpoint_segment_count = 80;                     // 80个segment (80GB)
};
```

---

## 13. 模块划分

```
spdk_engine/
├── include/
│   └── spdk_kv.h              # 公共API头文件
├── src/
│   ├── engine.cpp             # 引擎核心实现
│   ├── engine.h
│   ├── mem_index.cpp          # 内存索引 (Robin Hood Hashing)
│   ├── mem_index.h
│   ├── index_loader.cpp       # 索引加载器
│   ├── index_loader.h
│   ├── append_buffer.cpp      # 写入合并buffer
│   ├── append_buffer.h
│   ├── io_submitter.cpp       # IO批量提交
│   ├── io_submitter.h
│   ├── data_file.cpp          # 数据文件管理
│   ├── data_file.h
│   ├── superblock.cpp         # Superblock管理
│   ├── superblock.h
│   ├── compaction.cpp         # 空间回收任务
│   ├── compaction.h
│   ├── compaction_scheduler.cpp  # Compaction调度 (CPU控制)
│   ├── compaction_scheduler.h
│   ├── blob_manager.cpp       # SPDK Blob管理封装
│   ├── blob_manager.h
│   ├── dma_memory_pool.cpp    # DMA内存池 (NUMA感知)
│   ├── dma_memory_pool.h
│   └── utils.cpp              # 工具函数
├── tests/
│   ├── unit/
│   └── integration/
├── CMakeLists.txt
└── README.md
```

---

## 14. 后续迭代方向

### 14.1 功能增强
1. **Range Query支持**: 增加范围查询能力
2. **多Engine实例**: 支持单进程多Engine实例
3. **快照功能**: 支持创建一致性快照
4. **压缩支持**: 对value进行透明压缩
5. **加密支持**: 支持数据加密存储
6. **RDMA深度集成**: 提供原生RDMA接口 (部分已在 7.8 节实现)
7. **Adaptive Compaction**: 根据负载自动调整Compaction策略

### 14.2 极致性能优化 (可选)

**I-Cache 友好性优化 (实验性)**:

通过链接脚本将热路径函数链接到相邻内存页，最大化 I-Cache 命中率。

```ld
/* hot_functions.ld - 链接脚本片段 */
SECTIONS
{
    .text.hot : {
        /* 将热路径函数放在相邻位置 */
        *(.text.hot.poll)
        *(.text.hot.append)
        *(.text.hot.find)
        *(.text.hot.upsert)
        *(.text.hot.get)
        *(.text.hot.put)
    }
}
```

```cpp
// 使用 __attribute__ 标记热路径函数
__attribute__((section(".text.hot.poll")))
void Engine::poll() { /* ... */ }

__attribute__((section(".text.hot.find")))
MemIndexEntry* MemIndex::find(uint64_t key) { /* ... */ }
```

**评估说明**:
- **优点**: 可能提升 I-Cache 命中率 5-10%
- **缺点**:
  - 实现复杂，需要维护链接脚本
  - 效果高度依赖具体工作负载
  - 调试困难，符号地址不直观
  - 可能与 LTO (Link Time Optimization) 冲突
- **建议**: 仅在性能分析明确显示 I-Cache Miss 是瓶颈时考虑

**其他极致优化方向**:
1. **PGO (Profile-Guided Optimization)**: 基于实际运行数据优化代码布局
2. **BOLT**: Facebook 的二进制优化工具，可在链接后优化代码布局
3. **分支预测提示**: 使用 `__builtin_expect` 标记热路径分支
