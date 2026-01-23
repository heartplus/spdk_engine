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

```
┌─────────────────────────────────────────────────────────────────┐
│                    单核 Polling 循环                              │
├─────────────────────────────────────────────────────────────────┤
│  while (running) {                                              │
│      // 1. 处理前台 IO 完成                                       │
│      spdk_nvme_qpair_process_completions(foreground_qpair_);    │
│                                                                 │
│      // 2. 处理 AppendBuffer 提交                                │
│      process_append_buffers();                                  │
│                                                                 │
│      // 3. 处理用户请求 (put/get/del)                            │
│      process_pending_requests();                                │
│                                                                 │
│      // 4. 检查 Checkpoint 触发条件                               │
│      check_checkpoint_trigger();                                │
│                                                                 │
│      // 5. 推进 Compaction (低优先级)                             │
│      compaction_scheduler_.poll();                              │
│  }                                                              │
└─────────────────────────────────────────────────────────────────┘
```

**操作交错执行规则**:

| 操作 | 与 put/get 的关系 | 说明 |
|------|------------------|------|
| **Recovery** | **互斥** | Engine 处于 RECOVERING 状态时不接受任何 put/get 请求，恢复完成后转为 READY 状态 |
| **Compaction** | **可交错** | Compaction 在同一 polling 循环中与 put/get 交替执行，但前台 IO 优先级更高 |
| **Checkpoint** | **可交错** | Checkpoint 异步执行，不阻塞 put/get；采用 Copy-on-Write 保证一致性 |

**关键保证**:

1. **无并发竞争**: 所有操作在同一线程执行，无需锁或原子操作（除 RDMA 引用计数外）
2. **前台优先**: 当有 pending 的 put/get 请求时，Compaction IO 让步
3. **状态机控制**: Engine 状态机确保 Recovery 期间不会处理业务请求

```cpp
// Engine 状态决定是否接受请求
bool Engine::can_accept_request() const {
    return state_ == EngineState::READY;
}

// put/get 入口检查
int Engine::put(uint64_t key, void* value, uint32_t len, spdk_kv_cb cb, void* cb_arg) {
    if (!can_accept_request()) {
        return -EAGAIN;  // Engine 未就绪，拒绝请求
    }
    // ... 处理请求
}
```

### 1.3 基本接口

```cpp
// 创建新的engine实例
int spdk_kv_create(const char* dev_name, spdk_kv_create_opts* opts,
                   spdk_kv_cb cb, void* cb_arg);

// 打开已存在的engine
int spdk_kv_open(const char* dev_name, spdk_kv_open_opts* opts,
                 spdk_kv_cb cb, void* cb_arg);

// 写入KV对
int spdk_kv_put(spdk_kv_handle handle, uint64_t key,
                void* value, uint32_t value_len,
                spdk_kv_cb cb, void* cb_arg);

// 读取Value
int spdk_kv_get(spdk_kv_handle handle, uint64_t key,
                void* value_buf, uint32_t buf_len,
                spdk_kv_cb cb, void* cb_arg);

// 删除KV对
int spdk_kv_del(spdk_kv_handle handle, uint64_t key,
                spdk_kv_cb cb, void* cb_arg);

// 关闭engine
int spdk_kv_close(spdk_kv_handle handle, spdk_kv_cb cb, void* cb_arg);
```

---

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

## 4. 写入合并 (Append Buffer)

### 4.1 设计背景

频繁下发小IO (如4KB) 无法充分利用NVMe吞吐优势。引入Append Buffer将多个写入请求合并为大IO。

### 4.2 动态Buffer管理架构

**设计背景**:
- 固定大小的 buffer (2MB × 4) 在写入突发时可能不足
- 需要动态调整 buffer 数量和 backpressure 机制通知客户端

使用 `spdk_ring` 实现动态Buffer管理：

```cpp
class AppendBufferManager {
public:
    // 基础配置
    static constexpr size_t BUFFER_SIZE = 2 * 1024 * 1024;  // 2MB (Hugepage)
    static constexpr size_t MIN_BUFFER_COUNT = 4;   // 最小buffer数量
    static constexpr size_t MAX_BUFFER_COUNT = 16;  // 最大buffer数量
    static constexpr size_t FLUSH_THRESHOLD = BUFFER_SIZE - 64 * 1024;

    // Backpressure 阈值
    static constexpr size_t BACKPRESSURE_HIGH_WATER = 12;  // 开始拒绝新请求
    static constexpr size_t BACKPRESSURE_LOW_WATER = 6;    // 恢复接受请求

    AppendBufferManager(DmaMemoryPool* pool)
        : pool_(pool), current_buffer_count_(MIN_BUFFER_COUNT)
        , backpressure_active_(false), was_backpressure_active_(false) {
        // 创建无锁ring用于buffer传递 (预留最大容量)
        submit_ring_ = spdk_ring_create(SPDK_RING_TYPE_MP_SC,
                                        MAX_BUFFER_COUNT + 1, SPDK_ENV_SOCKET_ID_ANY);
        complete_ring_ = spdk_ring_create(SPDK_RING_TYPE_SP_MC,
                                          MAX_BUFFER_COUNT + 1, SPDK_ENV_SOCKET_ID_ANY);

        // 创建恢复通知队列 (用于 backpressure 解除时的异步通知)
        resume_notify_ring_ = spdk_ring_create(SPDK_RING_TYPE_MP_SC,
                                               256, SPDK_ENV_SOCKET_ID_ANY);

        // 初始化最小数量的buffer
        for (size_t i = 0; i < MIN_BUFFER_COUNT; i++) {
            auto buf = new AppendBuffer(pool->alloc(BUFFER_SIZE));
            spdk_ring_enqueue(complete_ring_, (void**)&buf, 1, nullptr);
        }

        // 获取当前活跃buffer
        spdk_ring_dequeue(complete_ring_, (void**)&active_buffer_, 1);
    }

    // 动态扩展buffer数量
    bool try_expand() {
        if (current_buffer_count_ >= MAX_BUFFER_COUNT) {
            return false;
        }

        void* mem = pool_->alloc(BUFFER_SIZE);
        if (!mem) {
            return false;
        }

        auto buf = new AppendBuffer(mem);
        spdk_ring_enqueue(complete_ring_, (void**)&buf, 1, nullptr);
        current_buffer_count_++;
        return true;
    }

    // 检查并更新backpressure状态
    bool check_backpressure() {
        size_t pending = pending_count();

        if (backpressure_active_) {
            // 当前处于backpressure状态，检查是否可以恢复
            if (pending <= BACKPRESSURE_LOW_WATER) {
                backpressure_active_ = false;
            }
        } else {
            // 检查是否需要触发backpressure
            if (pending >= BACKPRESSURE_HIGH_WATER) {
                // 先尝试扩展
                if (!try_expand()) {
                    backpressure_active_ = true;
                }
            }
        }
        return backpressure_active_;
    }

    // 获取backpressure状态 (供外部查询)
    bool is_backpressure_active() const {
        return backpressure_active_;
    }

    // 获取待提交buffer数量
    size_t pending_count() const {
        return spdk_ring_count(submit_ring_);
    }

    // 追加数据 (零拷贝: 直接写入DMA buffer)
    int64_t append(const void* data, size_t len) {
        if (active_buffer_->used() + len > FLUSH_THRESHOLD) {
            return -1;  // 需要先flush
        }
        return active_buffer_->append(data, len);
    }

    // 提交当前buffer到IO队列 (非阻塞)
    void submit_current_buffer() {
        if (active_buffer_->used() == 0) return;

        // 将满的buffer放入submit_ring
        spdk_ring_enqueue(submit_ring_, (void**)&active_buffer_, 1, nullptr);

        // 尝试获取空闲buffer
        if (spdk_ring_dequeue(complete_ring_, (void**)&active_buffer_, 1) == 0) {
            // 没有空闲buffer，需要等待
            active_buffer_ = nullptr;
        }
    }

    // IO提交线程调用: 批量获取待提交的buffer
    size_t get_pending_buffers(AppendBuffer** buffers, size_t max_count) {
        return spdk_ring_dequeue(submit_ring_, (void**)buffers, max_count);
    }

    // IO完成后归还buffer
    //
    // **重要**: 此实现同时适用于 RDMA 和非 RDMA 场景。
    // - 只有 can_reset() 返回 true (所有 RDMA slot 都已释放) 的 Buffer 才能 reset
    // - 否则需要加入 pending_reset_queue_ 等待后续检查
    //
    // 详见 7.9 节 "RDMA 场景下的 Buffer 生命周期锁定协议"
    //
    void return_buffer(AppendBuffer* buf) {
        // 检查 buffer 是否可以安全 reset
        // 在 RDMA 高并发写入时，如果不检查 ref_count，极易发生内存提前复用导致数据损坏
        if (!buf->can_reset()) {
            // 还有 RDMA slot 未释放，加入挂起队列等待
            pending_reset_queue_.push(buf);
            return;
        }

        // 所有引用已释放，可以安全 reset
        buf->reset();
        spdk_ring_enqueue(complete_ring_, (void**)&buf, 1, nullptr);

        // 如果当前没有活跃buffer，尝试获取
        if (active_buffer_ == nullptr) {
            spdk_ring_dequeue(complete_ring_, (void**)&active_buffer_, 1);
        }
    }

    // 在 polling 循环中检查挂起的 buffer
    // 必须在主循环中定期调用此函数，以处理 RDMA 场景下延迟释放的 buffer
    void check_pending_resets() {
        AppendBuffer* buf;
        while (pending_reset_queue_.try_pop(&buf)) {
            if (buf->can_reset()) {
                buf->reset();
                spdk_ring_enqueue(complete_ring_, (void**)&buf, 1, nullptr);
            } else {
                // 仍然不能 reset，放回队列
                pending_reset_queue_.push(buf);
                break;  // 避免死循环
            }
        }
    }

    // 在backpressure时使用：返回错误码通知客户端
    static constexpr int ERR_BACKPRESSURE = -EAGAIN;

    // 带backpressure检查的预留空间
    void* reserve_with_backpressure(size_t len, int* error) {
        if (check_backpressure()) {
            *error = ERR_BACKPRESSURE;
            return nullptr;
        }

        void* slot = reserve(len);
        if (!slot) {
            *error = ERR_BACKPRESSURE;
        }
        return slot;
    }

    // ========== 异步通知机制 (优化) ==========

    // 注册 Backpressure 恢复回调
    using BackpressureCallback = void (*)(void* arg);

    void register_resume_callback(BackpressureCallback cb, void* arg) {
        ResumeNotification notification{cb, arg};
        spdk_ring_enqueue(resume_notify_ring_, (void**)&notification, 1, nullptr);
    }

    // 检查并触发恢复通知 (在主循环中调用)
    void process_resume_notifications() {
        if (!was_backpressure_active_ && !backpressure_active_) {
            return;  // 状态无变化
        }

        if (was_backpressure_active_ && !backpressure_active_) {
            // Backpressure 刚刚解除，通知所有等待者
            ResumeNotification notifications[64];
            size_t count = spdk_ring_dequeue(resume_notify_ring_,
                                             (void**)notifications, 64);

            for (size_t i = 0; i < count; i++) {
                notifications[i].callback(notifications[i].arg);
            }
        }

        was_backpressure_active_ = backpressure_active_;
    }

private:
    struct ResumeNotification {
        BackpressureCallback callback;
        void* arg;
    };

    struct spdk_ring* resume_notify_ring_;  // 恢复通知队列
    bool was_backpressure_active_ = false;  // 上一次的状态

public:
    // ========== RDMA 流量控制集成 ==========

    // RDMA Credit-based Flow Control 适配
    // 不直接返回错误，而是暂不消耗接收令牌
    class RdmaFlowController {
    public:
        RdmaFlowController(AppendBufferManager* mgr) : buffer_mgr_(mgr) {}

        // 检查是否可以处理新的 RDMA 请求
        // 返回 true 表示可以处理，false 表示应暂缓 (不消耗 credit)
        bool can_accept_request() {
            return !buffer_mgr_->is_backpressure_active();
        }

        // 尝试处理请求，如果 backpressure 则注册回调
        bool try_process_or_defer(void* rdma_ctx,
                                  BackpressureCallback on_resume) {
            if (can_accept_request()) {
                return true;  // 可以立即处理
            }

            // 注册恢复回调，暂不消耗 RDMA credit
            buffer_mgr_->register_resume_callback(on_resume, rdma_ctx);
            return false;
        }

    private:
        AppendBufferManager* buffer_mgr_;
    };

    RdmaFlowController* create_flow_controller() {
        return new RdmaFlowController(this);
    }

private:
    DmaMemoryPool* pool_;
    struct spdk_ring* submit_ring_;    // 待提交buffer队列
    struct spdk_ring* complete_ring_;  // 空闲buffer队列
    AppendBuffer* active_buffer_;      // 当前活跃buffer
    size_t current_buffer_count_;      // 当前buffer数量
    bool backpressure_active_;         // backpressure状态

    // RDMA 场景下的延迟释放队列
    // 当 buffer 的 RDMA slot 尚未全部释放时，暂存于此队列
    // 需要在主循环中定期调用 check_pending_resets() 处理
    std::queue<AppendBuffer*> pending_reset_queue_;

    // reserve() 为私有方法，防止外部直接调用绕过 Backpressure 检查
    // 外部应始终使用 reserve_with_backpressure()
    void* reserve(size_t len) {
        if (active_buffer_ == nullptr ||
            active_buffer_->used() + len > FLUSH_THRESHOLD) {
            return nullptr;
        }
        return active_buffer_->reserve(len);
    }
};
```

### 4.3 Batching IO Submission

在Polling循环中批量提交IO，减少MMIO写寄存器次数：

```cpp
class IoSubmitter {
public:
    static constexpr size_t BATCH_SIZE = 32;  // 批量提交大小

    void poll() {
        // 1. 批量获取待提交的buffer
        AppendBuffer* buffers[BATCH_SIZE];
        size_t count = buffer_manager_->get_pending_buffers(buffers, BATCH_SIZE);

        if (count == 0) return;

        // 2. 准备批量IO请求
        for (size_t i = 0; i < count; i++) {
            auto ctx = prepare_write_context(buffers[i]);
            pending_ios_.push_back(ctx);
        }

        // 3. 批量提交 (一次性写寄存器)
        submit_batch();
    }

private:
    void submit_batch() {
        if (pending_ios_.empty()) return;

        // 使用底层NVMe API批量提交
        for (auto& ctx : pending_ios_) {
            spdk_blob_io_write(current_file_->blob,
                               io_channel_,
                               ctx->buffer->data(),
                               ctx->offset / 4096,
                               ctx->size / 4096,
                               on_write_complete, ctx);
        }

        // 刷新提交队列 (触发MMIO doorbell)
        spdk_nvme_qpair_process_completions(qpair_, 0);

        pending_ios_.clear();
    }
};
```

### 4.4 真正零拷贝写入流程

**设计背景**:
- 原设计中 `build_entry(key, value, len, temp_buffer_)` 存在一次内存拷贝
- 在 50 万 IOPS、平均 Value 4KB 场景下，每秒产生 2GB 的内存拷贝量
- 采用原地构建或矢量写入 (Writev) 消除这次拷贝

**方案一：原地构建 (推荐)**

```cpp
struct PendingWrite {
    uint64_t key;
    uint32_t sequence;           // 写入序列号 (扩展为32bit)
    uint32_t buffer_offset;      // 在AppendBuffer中的偏移
    uint32_t aligned_size;       // 对齐后的大小
    spdk_kv_cb callback;
    void* cb_arg;
};

// 直接在AppendBuffer中原地构建Entry，避免memcpy
void Engine::put(uint64_t key, void* value, uint32_t len,
                 spdk_kv_cb cb, void* cb_arg) {
    // 1. 分配序列号
    uint32_t seq = allocate_sequence();

    // 2. 计算所需空间
    size_t header_size = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t);  // 20 bytes
    size_t entry_size = header_size + len + sizeof(uint32_t);  // +checksum
    size_t aligned_size = ALIGN_UP(entry_size, 4096);

    // 3. 尝试在active_buffer_中预留空间 (带Backpressure检查)
    int error = 0;
    void* slot = buffer_manager_->reserve_with_backpressure(aligned_size, &error);

    if (slot == nullptr) {
        if (error == AppendBufferManager::ERR_BACKPRESSURE) {
            // Backpressure激活，加入等待队列并注册恢复回调
            wait_queue_.push({key, value, len, seq, cb, cb_arg});
            buffer_manager_->register_resume_callback(on_backpressure_resume, this);
            return;
        }

        // Buffer满但未触发Backpressure，提交当前buffer后重试
        buffer_manager_->submit_current_buffer();

        slot = buffer_manager_->reserve_with_backpressure(aligned_size, &error);
        if (slot == nullptr) {
            // 仍然失败，加入等待队列
            wait_queue_.push({key, value, len, seq, cb, cb_arg});
            if (error == AppendBufferManager::ERR_BACKPRESSURE) {
                buffer_manager_->register_resume_callback(on_backpressure_resume, this);
            }
            return;
        }
    }

    // 4. 原地构建Entry (零拷贝)
    build_entry_inplace(slot, key, value, len, seq);

    // 5. 记录待完成写入
    uint32_t offset = buffer_manager_->get_offset(slot);
    pending_writes_.push_back({key, seq, offset, (uint32_t)aligned_size, cb, cb_arg});
}

// =========== RDMA 场景区分 ===========
//
// **设计约束** (重要):
// RDMA Write Put 模式要求客户端只能写入 Engine 预先分配的 AppendBuffer slot。
// 不支持"先 RDMA WRITE 到独立 RX buffer，再由 server 解析处理"的模式。
//
// 理由:
// 1. AppendBuffer 的生命周期由 AppendBufferManager 管控
// 2. flush 后 buffer 会 reset/reuse
// 3. 避免额外的数据拷贝，实现真正的零拷贝路径
//
// 工作流程:
// 1. Client 请求 Engine 分配 slot (获取 AppendBuffer 中的一段地址)
// 2. Engine 返回 slot 地址和 RDMA rkey
// 3. Client 通过 RDMA WRITE 将 value 数据写入 slot 的指定偏移
// 4. Client 发送 RPC 通知 Engine 完成写入
// 5. Engine 调用 build_entry_inplace_rdma 构建 header/checksum
//
// 场景一：本地 Put (LocalPut)
//   - value 是本地指针，需要 memcpy 到 DMA Buffer
//   - 使用 build_entry_inplace 函数
//
// 场景二：RDMA Write (RdmaWritePut)
//   - value 的数据已经通过 RDMA 写入到 Engine 提供的 AppendBuffer slot 中
//   - 引擎直接使用该 slot 地址，无需 memcpy
//   - 使用 build_entry_inplace_rdma 函数
//
// API 层面通过不同的函数或参数区分这两种模式

// 写入模式枚举
enum class PutMode {
    LOCAL,      // 本地 Put，value 是本地内存指针
    RDMA_WRITE  // RDMA Write，value 已在 DMA Buffer 中
};

// 原地构建 Entry (本地 Put 场景，需要 memcpy)
void Engine::build_entry_inplace(void* slot, uint64_t key,
                                 void* value, uint32_t len, uint32_t seq) {
    char* ptr = static_cast<char*>(slot);

    // Header (注意：EntryHeader 现在是 16 bytes)
    auto* header = reinterpret_cast<EntryHeader*>(ptr);
    header->magic = ENTRY_MAGIC;
    header->version = 1;
    header->flags = 0;
    header->reserved = 0;
    header->sequence = seq;  // 持久化序列号
    header->padding = 0;
    ptr += sizeof(EntryHeader);

    // Key
    *reinterpret_cast<uint64_t*>(ptr) = key;
    ptr += sizeof(uint64_t);

    // Value Length
    *reinterpret_cast<uint32_t*>(ptr) = len;
    ptr += sizeof(uint32_t);

    // Value (本地 Put: 需要 memcpy)
    memcpy(ptr, value, len);
    ptr += len;

    // Padding (清零)
    size_t used = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) + len;
    size_t aligned = ALIGN_UP(used + sizeof(uint32_t), 4096);
    size_t padding = aligned - used - sizeof(uint32_t);
    if (padding > 0) {
        memset(ptr, 0, padding);
        ptr += padding;
    }

    // Checksum
    *reinterpret_cast<uint32_t*>(ptr) = crc32(slot, ptr - static_cast<char*>(slot));
}

// 原地构建 Entry (RDMA Write 场景，value 已在 DMA Buffer 中，无需 memcpy)
void Engine::build_entry_inplace_rdma(void* slot, uint64_t key,
                                      void* value_in_dma, uint32_t len, uint32_t seq) {
    // RDMA 场景下，value 数据已经通过 RDMA WRITE 写入到 slot 的指定位置
    // 我们只需要构建 Header 和 Tail (checksum)

    char* ptr = static_cast<char*>(slot);

    // Header (注意：EntryHeader 现在是 16 bytes)
    auto* header = reinterpret_cast<EntryHeader*>(ptr);
    header->magic = ENTRY_MAGIC;
    header->version = 1;
    header->flags = 0;
    header->reserved = 0;
    header->sequence = seq;  // 持久化序列号
    header->padding = 0;
    ptr += sizeof(EntryHeader);

    // Key
    *reinterpret_cast<uint64_t*>(ptr) = key;
    ptr += sizeof(uint64_t);

    // Value Length
    *reinterpret_cast<uint32_t*>(ptr) = len;
    ptr += sizeof(uint32_t);

    // Value: 跳过，数据已经在 buffer 中 (由 RDMA WRITE 写入)
    ptr += len;

    // Padding (清零)
    size_t used = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t) + len;
    size_t aligned = ALIGN_UP(used + sizeof(uint32_t), 4096);
    size_t padding = aligned - used - sizeof(uint32_t);
    if (padding > 0) {
        memset(ptr, 0, padding);
        ptr += padding;
    }

    // Checksum (计算整个 Entry 的校验和)
    *reinterpret_cast<uint32_t*>(ptr) = crc32(slot, ptr - static_cast<char*>(slot));
}

// =========== AppendSlot 抽象层 ===========
//
// **设计原则**:
// - AppendBuffer 是内部实现细节，不直接暴露给协议层
// - AppendSlot (又称 WriteTicket) 是协议层抽象，用于管理写入生命周期
// - RDMA 协议通过 AppendSlot 与 Engine 交互，而非直接操作 AppendBuffer
//
// 这种分离的好处:
// 1. AppendBuffer 专注于内存管理和 IO 合并
// 2. AppendSlot 承载协议语义 (epoch、状态转换、两阶段提交)
// 3. 生命周期管理更清晰，避免 Buffer 被意外复用

struct AppendSlot {
    void* addr;            // 指向 AppendBuffer 内部的地址
    uint32_t len;          // 预留的总长度 (对齐后)
    uint32_t epoch;        // Buffer 的 epoch (用于检测 Buffer 复用)
    uint32_t slot_id;      // Slot 唯一标识

    // Slot 状态
    enum class State : uint8_t {
        ALLOCATED,         // 已分配，等待 RDMA WRITE
        RDMA_COMPLETE,     // RDMA WRITE 已完成，等待 COMMIT
        COMMITTED,         // 已提交，等待刷盘
        FLUSHED            // 已刷盘
    };
    State state;
};

// =========== RDMA 两阶段协议 ===========
//
// **Phase 1: 申请 Slot (REQUEST_PUT)**
//
//   Client ----[REQUEST_PUT(size)]----> Server
//   Server 从 active_buffer 预留空间，创建 AppendSlot
//   Server ----[SLOT_RESP(addr, rkey, slot_id, epoch)]----> Client
//
// **Phase 2: 写入 + 提交 (RDMA WRITE + COMMIT)**
//
//   Client ----[RDMA WRITE to slot.addr]----> Server (无需 Server 参与)
//   Client ----[COMMIT(slot_id)]----> Server
//
//   Server 收到 COMMIT 后:
//   1. 校验 slot_id 和 epoch (防止使用已失效的 slot)
//   2. 标记 slot.state = RDMA_COMPLETE
//   3. 构建 Header/Checksum
//   4. 当 Buffer 中所有 slot 都 COMMITTED 后，允许 flush
//
// **关键保证**:
// - Server 只有收到 COMMIT 后才认为 RDMA WRITE 完成
// - 未收到 COMMIT 的 slot 不会被 flush，避免写入损坏数据
// - epoch 机制防止使用已被回收的 Buffer 地址

// =========== RDMA Slot 分配接口 ===========
// RdmaSlot 是 AppendSlot 的协议层包装，增加 RDMA 特定字段

struct RdmaSlot {
    void* buffer;           // slot 起始地址 (= AppendSlot.addr)
    uint32_t value_offset;  // value 数据应写入的偏移 (= header_size)
    uint32_t max_value_len; // 最大可写入的 value 长度
    uint64_t rkey;          // RDMA remote key
    uint32_t slot_id;       // slot 标识，用于后续 COMMIT 调用
    uint32_t epoch;         // Buffer epoch (用于校验)
};

// 请求 RDMA 写入 slot (Phase 1: REQUEST_PUT)
// 返回 0 成功，-EAGAIN 表示 Backpressure，需等待后重试
int Engine::alloc_rdma_slot(uint32_t value_len, RdmaSlot* out_slot) {
    size_t header_size = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t);
    size_t entry_size = header_size + value_len + sizeof(uint32_t);
    size_t aligned_size = ALIGN_UP(entry_size, 4096);

    int error = 0;
    void* slot = buffer_manager_->reserve_with_backpressure(aligned_size, &error);

    if (slot == nullptr) {
        return error;  // -EAGAIN for backpressure
    }

    // 创建 AppendSlot 并注册
    uint32_t slot_id = allocate_slot_id(slot, aligned_size);
    uint32_t epoch = buffer_manager_->current_buffer_epoch();

    append_slots_[slot_id] = AppendSlot{
        .addr = slot,
        .len = static_cast<uint32_t>(aligned_size),
        .epoch = epoch,
        .slot_id = slot_id,
        .state = AppendSlot::State::ALLOCATED
    };

    // 填充返回结构
    out_slot->buffer = slot;
    out_slot->value_offset = header_size;
    out_slot->max_value_len = aligned_size - header_size - sizeof(uint32_t);
    out_slot->rkey = get_buffer_rkey(slot);
    out_slot->slot_id = slot_id;
    out_slot->epoch = epoch;

    return 0;
}

// COMMIT 接口 (Phase 2: 客户端发送 COMMIT 通知)
// 返回 0 成功，-EINVAL 表示 slot 无效或 epoch 不匹配
int Engine::commit_rdma_slot(uint32_t slot_id, uint32_t epoch) {
    auto it = append_slots_.find(slot_id);
    if (it == append_slots_.end()) {
        return -EINVAL;  // slot 不存在
    }

    AppendSlot& slot = it->second;

    // 校验 epoch (防止使用已被回收的 Buffer)
    if (slot.epoch != epoch) {
        return -EINVAL;  // epoch 不匹配，Buffer 可能已被复用
    }

    // 校验状态
    if (slot.state != AppendSlot::State::ALLOCATED) {
        return -EINVAL;  // 状态错误，不能重复 COMMIT
    }

    // 标记 RDMA WRITE 完成
    slot.state = AppendSlot::State::RDMA_COMPLETE;

    // 通知 AppendBuffer 该 slot 的 RDMA 已完成
    buffer_manager_->mark_rdma_complete(slot_id);

    return 0;
}

// =========== RDMA Put 生命周期约束 (重要) ===========
//
// **问题背景**:
// 1. AppendBufferManager 在 IO 完成后会 reset() Buffer 并重新分配
// 2. 如果 RDMA 传输延迟较高，或 Backpressure 导致 Buffer 积压，
//    可能在 RDMA 传输完成前 Buffer 就被复用，导致数据污染
//
// **设计约束**:
// 1. alloc_rdma_slot 分配的 slot 具有"租约"属性，必须通过引用计数保护
// 2. 只有当 SPDK IO 完成 且 RDMA WRITE 完成通知到达后，才能释放 slot
// 3. put_rdma 调用前，调用方必须确保 RDMA WRITE 已完成
//
// **详细的生命周期锁定协议见 7.9 节**

// RDMA Put 接口 (用于 RDMA 场景，在 alloc_rdma_slot 之后调用)
//
// **前置条件** (调用方必须保证):
// 1. dma_buffer 是由 alloc_rdma_slot 返回的有效地址
// 2. RDMA WRITE 操作已完成 (客户端已收到 RDMA 完成通知)
// 3. value 数据已完整写入到 dma_buffer + value_offset 位置
//
// **重要**: 如果在 RDMA WRITE 完成前调用此函数，下发到磁盘的数据将是损坏的
//
void Engine::put_rdma(uint64_t key, void* dma_buffer, uint32_t value_offset,
                      uint32_t len, uint32_t seq,
                      spdk_kv_cb cb, void* cb_arg) {
    // dma_buffer: 已经包含 value 数据的 DMA Buffer 地址 (由 alloc_rdma_slot 返回)
    // value_offset: value 数据在 dma_buffer 中的偏移
    // 调用者负责确保数据已经通过 RDMA WRITE 写入

    // 计算所需空间
    size_t header_size = sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t);
    size_t entry_size = header_size + len + sizeof(uint32_t);
    size_t aligned_size = ALIGN_UP(entry_size, 4096);

    // 验证 dma_buffer 布局是否正确
    // (期望 value 数据位于 header_size 偏移处)
    if (value_offset != header_size) {
        // 需要重新排列或报错
        cb(cb_arg, -EINVAL);
        return;
    }

    // 构建 Entry (无需 memcpy value)
    build_entry_inplace_rdma(dma_buffer, key, nullptr, len, seq);

    // 提交 IO
    // ...
}
```

**方案二：矢量写入 (适用于Value已在独立RDMA Buffer中)**

```cpp
// 利用 spdk_blob_io_writev 将 Header 和 Value 分开但在IO层面合并提交
void Engine::put_vectored(uint64_t key, void* value, uint32_t len,
                          spdk_kv_cb cb, void* cb_arg) {
    uint32_t seq = allocate_sequence();

    // 从小buffer池分配Header空间 (仅64字节)
    void* header_buf = pools_.small_pool.alloc_tiny(64);
    build_header_only(header_buf, key, len, seq);

    // 构建iovec
    struct iovec iov[3];
    iov[0].iov_base = header_buf;
    iov[0].iov_len = 20;  // Header + Key + ValueLen

    iov[1].iov_base = value;  // 用户的RDMA buffer (零拷贝)
    iov[1].iov_len = len;

    // Padding + Checksum (预分配的小buffer)
    size_t tail_size = ALIGN_UP(20 + len + 4, 4096) - 20 - len;
    void* tail_buf = pools_.small_pool.alloc_tiny(tail_size);
    prepare_tail(tail_buf, tail_size, header_buf, value, len);
    iov[2].iov_base = tail_buf;
    iov[2].iov_len = tail_size;

    // 矢量写入
    auto* ctx = new WritevContext{key, seq, header_buf, tail_buf, cb, cb_arg};
    spdk_blob_io_writev(current_file_->blob, io_channel_,
                        iov, 3,
                        current_file_->write_offset / 4096,
                        ALIGN_UP(20 + len + tail_size, 4096) / 4096,
                        on_writev_complete, ctx);
}
```

### 4.5 刷盘触发条件（增强版）

**基础触发**:
1. **Buffer满**: 达到FLUSH_THRESHOLD
2. **超时**: 距离上次刷盘超过指定时间 (如10ms)
3. **显式请求**: 用户调用sync接口
4. **关闭引擎**: 优雅关闭时

**高IOPS场景增强触发** (解决submit_ring积压问题):
```cpp
class FlushTrigger {
public:
    // 基础阈值
    static constexpr size_t BUFFER_THRESHOLD = 16;   // submit_ring中积压超过16个Buffer
    static constexpr size_t ENTRY_THRESHOLD = 128;   // 当前buffer中累计超过128条Entry

    // 检查是否需要立即触发提交
    bool should_flush_immediately(size_t pending_buffers, size_t pending_entries) {
        return pending_buffers >= BUFFER_THRESHOLD ||
               pending_entries >= ENTRY_THRESHOLD;
    }
};

// 在Polling循环中集成
void Engine::poll() {
    // 1. 检查是否需要立即刷盘
    size_t pending_buffers = buffer_manager_->pending_count();
    size_t pending_entries = buffer_manager_->current_entry_count();

    if (flush_trigger_.should_flush_immediately(pending_buffers, pending_entries)) {
        // 立即提交当前buffer
        buffer_manager_->submit_current_buffer();
    }

    // 2. 批量获取待提交的buffer
    AppendBuffer* buffers[BATCH_SIZE];
    size_t count = buffer_manager_->get_pending_buffers(buffers, BATCH_SIZE);

    if (count > 0) {
        // 3. 批量提交IO
        submit_batch(buffers, count);

        // 4. 立即触发doorbell (不等待下一轮polling)
        spdk_nvme_qpair_process_completions(qpair_, 0);
    }

    // 5. 处理完成的IO
    process_completions();

    // 6. 处理等待队列中的请求
    process_wait_queue();
}
```

---

## 5. 内存索引设计 (详细)

### 5.1 优化的索引条目结构

为冲刺50万IOPS，优化MemIndexEntry结构。

**序列号回绕问题分析**:
- 16bit 序列号在 50 万 IOPS 下仅需 0.13 秒就会回绕
- 24bit 序列号在 50 万 IOPS 下约 33 秒回绕
- 32bit 序列号在 50 万 IOPS 下约 2.4 小时回绕，在 100 万 IOPS 下约 1.2 小时回绕
- 解决方案：扩展序列号到 32bit，提供足够的安全余量

```cpp
// 优化后的20字节索引条目 (32bit序列号)
struct MemIndexEntry {
    uint64_t key;                    // 8 bytes: 用户key

    // 位域打包 (4 bytes)
    uint32_t file_id      : 10;      // 文件ID (支持1024个文件)
    uint32_t offset_index : 21;      // 文件内偏移索引 (以4KB为单位, 支持8GB)
    uint32_t deleted      : 1;       // 删除标记

    // 序列号 (4 bytes) - 扩展到完整 32bit
    uint32_t sequence;               // 写入序列号 (32bit，约2.4小时回绕@50万IOPS)

    // page_count 和 tag (4 bytes)
    uint16_t page_count;             // value占用的4KB页面数 (最大256MB)
    uint8_t  tag;                    // 哈希指纹 (用于快速过滤)
    uint8_t  reserved;               // 保留字段
};  // 总计: 20 bytes (对齐到4字节边界)

static_assert(sizeof(MemIndexEntry) == 20, "MemIndexEntry must be 20 bytes");
```

**备选方案：保持16字节但使用混合序列号**

如果内存极度敏感，可使用文件级序列号 + 局部序列号的方案：

```cpp
// 16字节版本 (使用混合序列号)
struct MemIndexEntry16 {
    uint64_t key;                    // 8 bytes: 用户key

    // 位域打包 (4 bytes)
    uint32_t file_id      : 10;      // 文件ID
    uint32_t offset_index : 21;      // 文件内偏移索引
    uint32_t deleted      : 1;       // 删除标记

    // 混合字段 (4 bytes)
    // 高16bit: 文件epoch (每次文件切换递增)
    // 低16bit: 文件内局部序列号
    uint32_t epoch_sequence;         // 混合序列号
};

// 序列号比较逻辑
static bool sequence_newer_hybrid(uint32_t a, uint32_t b) {
    // 先比较epoch (高16bit)，再比较局部序列号 (低16bit)
    uint16_t epoch_a = a >> 16;
    uint16_t epoch_b = b >> 16;

    if (epoch_a != epoch_b) {
        return (int16_t)(epoch_a - epoch_b) > 0;
    }
    return (int16_t)((a & 0xFFFF) - (b & 0xFFFF)) > 0;
}
```

**推荐选择**: 20字节方案
- 增加约 20% 内存 (32GB -> 40GB)
- 换取 200 倍的序列号空间 (0.13s -> 33s 回绕周期)
- 在极高负载下更安全

**字段说明**:
- `file_id`: 10bit，支持最多1024个数据文件
- `offset_index`: 21bit，以4KB为单位，支持8GB文件内寻址 (2^21 * 4KB = 8GB)
- `deleted`: 1bit，删除标记
- `sequence`: 32bit，写入序列号 (回绕周期约2.4小时@50万IOPS，约1.2小时@100万IOPS)
- `tag`: 8bit，key哈希的高8位，用于快速过滤不匹配的key
- `page_count`: 16bit，value占用的4KB页面数，最大支持256MB

### 5.2 Robin Hood Hashing 实现

采用 Robin Hood Hashing 减少最坏情况下的探测次数，保证P99延迟稳定：

```cpp
class MemIndex {
public:
    // 创建时指定容量，一次性分配所有内存
    // 负载因子降至0.55，牺牲约10GB内存换取更短的探测路径
    // 原因：20亿条记录在0.7负载因子下PSL显著增加，导致Cache Miss剧增
    explicit MemIndex(uint64_t max_entries, double load_factor = 0.55)
        : capacity_(next_power_of_2(static_cast<uint64_t>(max_entries / load_factor)))  // 负载因子0.55
        , size_(0)
        , global_sequence_(0) {
        // 分配并清零
        entries_ = static_cast<MemIndexEntry*>(
            aligned_alloc(64, capacity_ * sizeof(MemIndexEntry)));
        memset(entries_, 0, capacity_ * sizeof(MemIndexEntry));

        // 初始化PSL数组 (Probe Sequence Length)
        psl_ = static_cast<uint8_t*>(calloc(capacity_, 1));
    }

    ~MemIndex() {
        free(entries_);
        free(psl_);
    }

    // 计算哈希值和tag
    static inline void compute_hash(uint64_t key, uint64_t* hash, uint8_t* tag) {
        *hash = xxhash64(key);
        *tag = (*hash >> 56) & 0xFF;  // 高8位作为tag
    }

    // 查找 (带Prefetch优化)
    MemIndexEntry* find(uint64_t key) {
        uint64_t hash;
        uint8_t tag;
        compute_hash(key, &hash, &tag);

        uint64_t idx = hash & (capacity_ - 1);
        uint8_t dist = 0;

        // Prefetch: 预取哈希桶地址
        __builtin_prefetch(&entries_[idx], 0, 3);

        while (true) {
            // 如果当前位置的PSL小于我们的探测距离，说明key不存在
            if (psl_[idx] < dist) {
                return nullptr;
            }

            MemIndexEntry& entry = entries_[idx];

            // 快速tag过滤 (避免完整key比较)
            if (entry.tag == tag && entry.key == key && !entry.deleted) {
                return &entry;
            }

            // 线性探测下一个位置
            idx = (idx + 1) & (capacity_ - 1);
            dist++;

            // Prefetch下一个位置
            __builtin_prefetch(&entries_[(idx + 1) & (capacity_ - 1)], 0, 3);

            // 安全检查: 防止无限循环
            if (dist > 128) {
                return nullptr;  // 异常情况
            }
        }
    }

    // 插入或更新 (Robin Hood Hashing)
    //
    // **重要**: 此函数直接使用传入 new_entry 的所有字段，包括 sequence。
    // 调用方需要确保 sequence 字段已正确设置：
    // - 正常写入: 调用方先调用 allocate_sequence() 获取新序列号，设置到 entry 中
    // - 恢复写入: 调用方使用从磁盘读取的持久化序列号，直接设置到 entry 中
    //
    // 此设计保证了：
    // 1. 正常运行时，新写入总是获得递增的序列号
    // 2. 恢复时，使用持久化的序列号保持原有顺序
    // 3. 无论哪种场景，upsert 内部的序列号比较逻辑一致
    //
    bool upsert(uint64_t key, const MemIndexEntry& new_entry) {
        uint64_t hash;
        uint8_t tag;
        compute_hash(key, &hash, &tag);

        uint64_t idx = hash & (capacity_ - 1);
        uint8_t dist = 0;

        MemIndexEntry entry_to_insert = new_entry;
        entry_to_insert.tag = tag;

        while (true) {
            // 空槽位，直接插入
            if (psl_[idx] == 0 && entries_[idx].key == 0) {
                entries_[idx] = entry_to_insert;
                psl_[idx] = dist + 1;  // PSL从1开始
                size_++;
                return true;
            }

            // 找到相同key，更新
            if (entries_[idx].key == key) {
                // 检查序列号: 只有新 entry 的 sequence 更大时才更新
                // 这保证了恢复时不会出现旧值覆盖新值的问题
                if (sequence_newer(entry_to_insert.sequence, entries_[idx].sequence)) {
                    entries_[idx] = entry_to_insert;
                }
                return true;
            }

            // Robin Hood: 如果当前entry的PSL比我们小，交换
            if (psl_[idx] < dist + 1) {
                std::swap(entries_[idx], entry_to_insert);
                std::swap(psl_[idx], dist);
                dist++;  // 被换出的entry继续探测
            }

            idx = (idx + 1) & (capacity_ - 1);
            dist++;

            // 负载过高检查
            if (dist > 128) {
                return false;  // 需要rehash
            }
        }
    }

    // 删除 (标记删除，不实际移除)
    //
    // **重要**: 此函数仅更新内存索引的删除标记，不涉及磁盘操作。
    // 调用方在调用此函数后，**必须**向 AppendBuffer 写入一条符合 3.4 节格式
    // (Tombstone) 的物理删除记录，否则重启后删除操作会丢失。
    //
    // 完整的删除流程:
    // 1. 调用 remove() 标记内存索引为已删除
    // 2. 向 AppendBuffer 写入 Tombstone 记录 (flags 的 bit0 = 1)
    // 3. 等待 Tombstone 记录持久化完成后，才能向用户返回成功
    //
    // 恢复时: 从数据文件加载 Tombstone 记录，重新执行删除标记
    //
    bool remove(uint64_t key) {
        MemIndexEntry* entry = find(key);
        if (entry) {
            entry->deleted = 1;
            return true;
        }
        return false;
    }

    // 分配序列号 (32bit，回绕周期约2.4小时@50万IOPS)
    uint32_t allocate_sequence() {
        return ++global_sequence_;  // 完整 32bit，自然回绕
    }

    // 设置全局序列号 (用于恢复时)
    void set_global_sequence(uint32_t seq) {
        global_sequence_ = seq;
    }

    // 序列号比较 (处理32bit回绕)
    static bool sequence_newer(uint32_t a, uint32_t b) {
        // 使用有符号差值判断，处理回绕
        // 假设两个序列号之间的差值不会超过 2^31
        return (int32_t)(a - b) > 0;
    }

    // 获取统计信息
    uint64_t size() const { return size_; }
    uint64_t capacity() const { return capacity_; }
    double load_factor() const { return (double)size_ / capacity_; }

private:
    MemIndexEntry* entries_;      // 索引数组
    uint8_t* psl_;                // Probe Sequence Length数组
    uint64_t capacity_;           // 容量 (2的幂)
    uint64_t size_;               // 当前条目数
    uint32_t global_sequence_;    // 全局序列号 (32bit，约2.4小时回绕@50万IOPS)

    static uint64_t next_power_of_2(uint64_t n) {
        n--;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        n |= n >> 32;
        return n + 1;
    }
};
```

**正常写入与恢复写入的调用方式**:

```cpp
// === 正常写入 (运行时) ===
// 调用方负责分配序列号
void Engine::put_async(uint64_t key, void* value, size_t len, spdk_kv_cb cb, void* cb_arg) {
    MemIndexEntry entry;
    entry.key = key;
    entry.file_id = current_file_->file_id;
    entry.offset_index = current_offset_ / 4096;
    entry.page_count = (len + 4095) / 4096;
    entry.deleted = 0;
    entry.sequence = mem_index_->allocate_sequence();  // 分配新序列号

    mem_index_->upsert(key, entry);
    // ... 异步写入数据
}

// === 恢复写入 (启动时) ===
// 使用磁盘上持久化的序列号
void IndexLoader::parse_and_rebuild_entries() {
    // ...
    MemIndexEntry new_entry;
    new_entry.key = key;
    new_entry.file_id = file.file_id;
    new_entry.offset_index = (current_scan_offset_ + offset) / 4096;
    new_entry.page_count = (entry_size + 4095) / 4096;
    new_entry.deleted = (header->flags & FLAG_DELETED) ? 1 : 0;
    new_entry.sequence = header->sequence;  // 使用持久化的序列号，不调用 allocate_sequence()

    mem_index_->upsert(key, new_entry);
    // ...
}
```

### 5.3 SIMD 加速哈希对比 (AVX2)

**设计背景**:
- 在 find 逻辑中，线性探测需要逐个比较 tag 和 key
- 利用 AVX2 指令集一次载入多个 MemIndexEntry 进行并行匹配，可显著减少探测次数
- 在 100 万 IOPS 下，任何能减少 CPU 指令的优化都很有价值

```cpp
#ifdef __AVX2__
#include <immintrin.h>

class MemIndexSIMD {
public:
    // AVX2 优化的查找 (一次比较 4 个 entry)
    // 注意：20字节的 MemIndexEntry 需要特殊处理，这里展示 tag 快速过滤
    MemIndexEntry* find_avx2(uint64_t key) {
        uint64_t hash;
        uint8_t tag;
        compute_hash(key, &hash, &tag);

        uint64_t idx = hash & (capacity_ - 1);

        // 将 tag 广播到 256-bit 寄存器的每个字节
        __m256i target_tag = _mm256_set1_epi8(tag);

        while (true) {
            // 确保 idx 对齐到 4 的倍数以便 SIMD 处理
            uint64_t aligned_idx = idx & ~3ULL;

            // 收集 4 个连续 entry 的 tag (假设 tag 在固定偏移位置)
            // 由于 MemIndexEntry 是 20 字节，需要 gather 指令或手动构造
            alignas(32) uint8_t tags[32] = {0};
            for (int i = 0; i < 4 && (aligned_idx + i) < capacity_; i++) {
                tags[i] = entries_[aligned_idx + i].tag;
            }

            // 加载 tags 到 SIMD 寄存器
            __m256i entry_tags = _mm256_loadu_si256((__m256i*)tags);

            // 比较 tag
            __m256i cmp_result = _mm256_cmpeq_epi8(entry_tags, target_tag);
            int mask = _mm256_movemask_epi8(cmp_result);

            // 检查低 4 位是否有匹配
            int match_mask = mask & 0xF;
            if (match_mask) {
                // 有 tag 匹配，进一步验证 key
                while (match_mask) {
                    int bit_pos = __builtin_ctz(match_mask);
                    uint64_t check_idx = aligned_idx + bit_pos;

                    if (check_idx < capacity_ &&
                        entries_[check_idx].key == key &&
                        !entries_[check_idx].deleted) {
                        return &entries_[check_idx];
                    }
                    match_mask &= (match_mask - 1);  // 清除最低位
                }
            }

            // 检查 PSL 是否需要继续探测
            uint8_t dist = (idx - aligned_idx) + 1;
            bool should_continue = false;
            for (int i = 0; i < 4 && (aligned_idx + i) < capacity_; i++) {
                if (psl_[aligned_idx + i] >= dist + i) {
                    should_continue = true;
                    break;
                }
            }

            if (!should_continue) {
                return nullptr;  // Key 不存在
            }

            idx = (aligned_idx + 4) & (capacity_ - 1);

            // 安全检查
            if (idx == (hash & (capacity_ - 1))) {
                return nullptr;  // 遍历了一圈
            }
        }
    }

    // 批量查找优化 (适用于批量 Get 请求)
    void batch_find_avx2(const uint64_t* keys, size_t count,
                         MemIndexEntry** results) {
        // 预计算所有 hash 和 tag
        alignas(32) uint64_t hashes[8];
        alignas(32) uint8_t tags[8];

        for (size_t batch_start = 0; batch_start < count; batch_start += 8) {
            size_t batch_size = std::min((size_t)8, count - batch_start);

            // 计算当前批次的 hash
            for (size_t i = 0; i < batch_size; i++) {
                compute_hash(keys[batch_start + i], &hashes[i], &tags[i]);
            }

            // Prefetch 所有目标位置
            for (size_t i = 0; i < batch_size; i++) {
                uint64_t idx = hashes[i] & (capacity_ - 1);
                __builtin_prefetch(&entries_[idx], 0, 3);
            }

            // 执行查找
            for (size_t i = 0; i < batch_size; i++) {
                results[batch_start + i] = find_avx2(keys[batch_start + i]);
            }
        }
    }
};

#else
// 非 AVX2 环境回退到标准实现
#define find_avx2 find
#define batch_find_avx2 batch_find
#endif
```

**运行时检测**:
```cpp
class SIMDCapability {
public:
    static bool has_avx2() {
        static bool checked = false;
        static bool supported = false;
        if (!checked) {
            unsigned int eax, ebx, ecx, edx;
            __cpuid_count(7, 0, eax, ebx, ecx, edx);
            supported = (ebx & (1 << 5)) != 0;  // AVX2 bit
            checked = true;
        }
        return supported;
    }
};

// 在 MemIndex 中根据 CPU 能力选择实现
MemIndexEntry* MemIndex::find(uint64_t key) {
    if (SIMDCapability::has_avx2()) {
        return find_avx2(key);
    }
    return find_scalar(key);
}
```

### 5.4 Prefetch优化策略

```cpp
// 批量查找时的Prefetch流水线
void batch_find(const uint64_t* keys, size_t count,
                MemIndexEntry** results) {
    constexpr size_t PREFETCH_DISTANCE = 8;

    // 预计算所有hash和index
    uint64_t hashes[count];
    uint64_t indices[count];
    uint8_t tags[count];

    for (size_t i = 0; i < count; i++) {
        compute_hash(keys[i], &hashes[i], &tags[i]);
        indices[i] = hashes[i] & (capacity_ - 1);
    }

    // 预取前PREFETCH_DISTANCE个
    for (size_t i = 0; i < std::min(count, PREFETCH_DISTANCE); i++) {
        __builtin_prefetch(&entries_[indices[i]], 0, 3);
    }

    // 流水线查找
    for (size_t i = 0; i < count; i++) {
        // 预取后续的
        if (i + PREFETCH_DISTANCE < count) {
            __builtin_prefetch(&entries_[indices[i + PREFETCH_DISTANCE]], 0, 3);
        }

        // 执行查找
        results[i] = find_internal(keys[i], indices[i], tags[i]);
    }
}
```

### 5.4 异步写入顺序问题解决方案

**问题场景**:
```
时间线:
T1: put(key, valueA) 开始IO, seq=100
T2: put(key, valueB) 开始IO, seq=101
T3: valueB IO完成，回调执行，索引更新为 key->valueB, seq=101
T4: valueA IO完成，回调执行，尝试更新索引
    检查: 100 > 101? No, 跳过更新  ← 正确!
```

### 5.5 Compaction与用户写入的一致性

**规则**: Compaction产生的Entry具有较低优先级

**is_compaction 参数来源**:

`is_compaction` 参数通过 `PendingWrite` 结构体的 flags 字段传递：

```cpp
struct PendingWrite {
    uint64_t key;
    uint32_t sequence;           // 写入序列号
    uint32_t buffer_offset;      // 在AppendBuffer中的偏移
    uint32_t aligned_size;       // 对齐后的大小
    uint8_t  flags;              // 标志位: bit0 = is_compaction
    spdk_kv_cb callback;
    void* cb_arg;

    // 辅助方法
    bool is_compaction() const { return flags & 0x01; }

    // 旧位置信息 (仅Compaction时有效)
    uint16_t old_file_id;
    uint32_t old_offset_index;
    MemIndexEntry new_entry;
};

// 用户写入时创建 PendingWrite
void Engine::put(...) {
    PendingWrite pw;
    pw.flags = 0;  // 用户写入，非Compaction
    // ...
}

// Compaction时创建 PendingWrite
void CompactionTask::migrate_entry(...) {
    PendingWrite pw;
    pw.flags = 0x01;  // Compaction写入
    pw.old_file_id = src_entry.file_id;
    pw.old_offset_index = src_entry.offset_index;
    // ...
}
```

**索引更新逻辑**:

```cpp
void update_index_on_write_complete(const PendingWrite& pw) {
    MemIndexEntry* existing = mem_index_->find(pw.key);

    if (existing == nullptr) {
        mem_index_->upsert(pw.key, pw.new_entry);
        return;
    }

    if (pw.is_compaction()) {
        // Compaction产生的更新，只在当前索引指向旧文件时才更新
        if (existing->file_id == pw.old_file_id &&
            existing->offset_index == pw.old_offset_index) {
            mem_index_->upsert(pw.key, pw.new_entry);
        }
    } else {
        // 用户写入，使用序列号判断
        if (MemIndex::sequence_newer(pw.sequence, existing->sequence)) {
            mem_index_->upsert(pw.key, pw.new_entry);
        }
    }
}
```

---

## 6. 内存索引序列化与加载 (详细)

### 6.1 增量 Checkpoint 机制

**设计背景**:
- MemIndex Area A/B 各 80GB，全量 Checkpoint 每次写 80GB 数据
- 在 50 万 IOPS 写负载下，每 300 秒做一次 Checkpoint 会增加约 250MB/s 的后台写入量
- 采用增量 Checkpoint 可大幅减少写放大

**分段设计**:
```cpp
// 统一的 Segment 配置常量 (全局使用)
// 注意：20字节 Entry × 20亿条 / 0.55负载因子 ≈ 72GB，划分为 1GB Segment
// Area A + Area B 各 80GB = 160GB 总空间，支持100万IOPS低冲突率
constexpr size_t MEM_INDEX_SEGMENT_SIZE = 1ULL * 1024 * 1024 * 1024;  // 1GB
constexpr size_t MEM_INDEX_SEGMENT_COUNT = 80;  // 80个Segment (每个Area 80个)，支持最大80GB索引

struct SegmentHeader {
    uint32_t magic;              // 0x53454748 ("SEGH")
    uint32_t segment_id;         // Segment编号 (0-79)
    uint64_t version;            // Segment版本号
    uint64_t entry_count;        // 本Segment有效条目数
    uint64_t dirty_sequence;     // 最后一次变更的全局序列号
    uint32_t checksum;           // Segment数据校验和
    uint8_t padding[28];         // 填充到64字节
};

class IncrementalCheckpoint {
public:
    // 脏位图: 每bit代表一个Segment是否有变更
    std::bitset<MEM_INDEX_SEGMENT_COUNT> dirty_segments_;

    // Copy-on-Write: Checkpoint开始时的脏位图快照
    std::bitset<MEM_INDEX_SEGMENT_COUNT> checkpoint_dirty_snapshot_;

    // 每个Segment的版本号
    uint64_t segment_versions_[MEM_INDEX_SEGMENT_COUNT];

    // 标记Segment为脏
    void mark_dirty(uint64_t bucket_index) {
        uint32_t segment_id = bucket_index / (capacity_ / MEM_INDEX_SEGMENT_COUNT);
        dirty_segments_.set(segment_id);
    }

    // 执行增量Checkpoint (采用 Copy-on-Write + 两阶段同步)
    //
    // **关键设计**: 所有快照数据必须在同一时刻原子采集，确保恢复一致性
    //
    // 在 Checkpoint 异步写入期间 (可能持续数秒)，active_buffer 可能发生多次切换 (Seal)。
    // 为保证恢复时扫描起点与索引镜像完全衔接，必须满足：
    // 1. checkpoint_start_sequence_ 在快照脏位图的一瞬间采集
    // 2. active_buffer_positions 快照与上述序列号同时冻结
    // 3. Superblock 更新时写入的是这些"快照值"，而非更新完成那一刻的"实时值"
    //
    void checkpoint_async(spdk_kv_cb cb, void* cb_arg) {
        // === 原子快照采集 ===
        // 以下三个操作必须在同一时刻完成，保证一致性：

        // 1. 记录 Checkpoint 开始时的全局序列号 (用于恢复时正确分配新序列号)
        checkpoint_start_sequence_ = global_sequence_;

        // 2. 冻结当前活跃 Buffer 位置快照 (恢复时从这些位置开始扫描)
        //    注意：必须与 checkpoint_start_sequence_ 同时采集
        snapshot_active_buffer_positions();

        // 3. Copy-on-Write: 快照当前脏位图，立即清空以接收新写入
        checkpoint_dirty_snapshot_ = dirty_segments_;
        dirty_segments_.reset();  // 清空，新写入会标记到新的位图中

        pending_segments_.clear();

        // 收集快照中的脏Segment
        for (uint32_t i = 0; i < MEM_INDEX_SEGMENT_COUNT; i++) {
            if (checkpoint_dirty_snapshot_.test(i)) {
                pending_segments_.push_back(i);
            }
        }

        if (pending_segments_.empty()) {
            cb(cb_arg, 0);
            return;
        }

        // 开始异步写入
        current_segment_idx_ = 0;
        checkpoint_callback_ = cb;
        checkpoint_cb_arg_ = cb_arg;
        write_next_segment();
    }

    // 快照当前活跃 Buffer 位置
    // 此快照将在 Superblock 更新时写入，确保恢复时扫描起点与索引镜像一致
    void snapshot_active_buffer_positions() {
        checkpoint_buffer_count_ = active_buffer_manager_->get_active_count();
        for (uint8_t i = 0; i < checkpoint_buffer_count_; i++) {
            auto* buffer = active_buffer_manager_->get_buffer(i);
            checkpoint_buffer_positions_[i].file_id = buffer->current_file_id();
            checkpoint_buffer_positions_[i].page_index = buffer->current_page_index();
        }
    }

private:
    void write_next_segment() {
        if (current_segment_idx_ >= pending_segments_.size()) {
            // 所有脏Segment写入完成
            // === 两阶段同步: 第一阶段 - 确保数据落盘 ===
            spdk_blob_sync_md(mem_index_blob_, on_sync_complete, this);
            return;
        }

        uint32_t seg_id = pending_segments_[current_segment_idx_];

        // 序列化Segment数据到buffer
        size_t data_size = serialize_segment(seg_id, segment_buffer_);

        // 计算写入位置 (在当前活跃的MemIndex Area中)
        uint64_t offset = seg_id * MEM_INDEX_SEGMENT_SIZE;

        spdk_blob_io_write(mem_index_blob_, channel_,
                          segment_buffer_, offset / 4096,
                          data_size / 4096,
                          on_segment_written, this);
    }

    static void on_segment_written(void* arg, int status) {
        auto* self = static_cast<IncrementalCheckpoint*>(arg);
        if (status != 0) {
            self->checkpoint_callback_(self->checkpoint_cb_arg_, status);
            return;
        }
        self->current_segment_idx_++;
        self->write_next_segment();
    }

    static void on_sync_complete(void* arg, int status) {
        auto* self = static_cast<IncrementalCheckpoint*>(arg);
        if (status != 0) {
            self->checkpoint_callback_(self->checkpoint_cb_arg_, status);
            return;
        }

        // === 两阶段同步: 第二阶段 - 更新 Superblock 作为原子标记 ===
        for (uint32_t seg_id : self->pending_segments_) {
            self->segment_versions_[seg_id]++;
        }

        // 更新 Superblock 中的 checkpoint 信息 (原子标记)
        // **关键**: 必须使用快照值，而非当前实时值
        // - checkpoint_start_sequence_: 快照采集时的全局序列号
        // - checkpoint_buffer_positions_: 快照采集时的活跃 Buffer 位置
        self->update_superblock_checkpoint_with_snapshot(
            self->checkpoint_start_sequence_,
            self->checkpoint_buffer_positions_,
            self->checkpoint_buffer_count_,
            [](void* arg, int status) {
                auto* self = static_cast<IncrementalCheckpoint*>(arg);
                self->checkpoint_callback_(self->checkpoint_cb_arg_, status);
            }, self);
    }

    std::vector<uint32_t> pending_segments_;
    size_t current_segment_idx_;
    spdk_kv_cb checkpoint_callback_;
    void* checkpoint_cb_arg_;
    void* segment_buffer_;
    uint32_t checkpoint_start_sequence_;  // Checkpoint开始时的全局序列号 (快照值)
    uint32_t global_sequence_;            // 当前全局序列号

    // 活跃 Buffer 位置快照 (Checkpoint 开始时采集，用于 Superblock 更新)
    struct ActiveBufferPos {
        uint16_t file_id;
        uint64_t page_index;
    } checkpoint_buffer_positions_[16];
    uint8_t checkpoint_buffer_count_;

    AppendBufferManager* active_buffer_manager_;  // 引用，用于快照采集
    uint64_t capacity_;                   // 哈希表容量
};
```

**Checkpoint触发策略**:
```cpp
struct CheckpointTrigger {
    // 周期性触发 (默认300秒)
    uint64_t interval_ns = 300ULL * 1000 * 1000 * 1000;

    // 写入量触发 (默认10GB)
    uint64_t bytes_threshold = 10ULL * 1024 * 1024 * 1024;

    // 脏Segment数量触发 (默认8个，即25%)
    uint32_t dirty_segment_threshold = 8;

    bool should_checkpoint(const IncrementalCheckpoint& ckpt,
                          uint64_t bytes_since_last,
                          uint64_t ns_since_last) {
        return ns_since_last >= interval_ns ||
               bytes_since_last >= bytes_threshold ||
               ckpt.dirty_segments_.count() >= dirty_segment_threshold;
    }
};
```

**Checkpoint 原子性保证**:

Checkpoint 的原子性边界以 Superblock 更新为准。只有当 Superblock 成功更新后，才认为本次 Checkpoint 完成。

```cpp
// Superblock 中必须包含的 Checkpoint 元信息
struct SuperblockCheckpointInfo {
    uint32_t checkpoint_global_seq;     // Checkpoint 时的全局序列号
    uint16_t checkpoint_file_id;        // Checkpoint 时的活跃文件 ID
    uint64_t checkpoint_page_index;     // Checkpoint 时的文件内 page 偏移
    uint64_t mem_index_area_version;    // MemIndex A/B 区域版本号
    uint8_t  active_mem_index_area;     // 当前活跃的 MemIndex Area (0=A, 1=B)
};
```

**崩溃恢复一致性分析**:

| 崩溃时机 | Superblock 状态 | MemIndex 状态 | 恢复策略 |
|---------|----------------|--------------|---------|
| Segment 写入中 | 旧版本 | 部分更新 | 使用旧 Superblock 指向的 MemIndex，从 checkpoint_page_index 开始增量恢复 |
| Segment sync 后、Superblock 更新前 | 旧版本 | 新版本 | 同上，新 Segment 数据会被忽略，重新从数据文件恢复 |
| Superblock 更新后 | 新版本 | 新版本 | 直接使用新 MemIndex，从新的 checkpoint_page_index 开始增量恢复 |

**关键原则**:
1. **原子提交单元**: MemIndex Segment 数据 + Superblock 共同构成一个原子提交单元
2. **恢复一致性**: 恢复时以 Superblock 记录的 checkpoint_global_seq 和 checkpoint_page_index 为准
3. **幂等性**: 增量恢复可以重复执行，因为 upsert 使用序列号比较，相同或更旧的记录不会覆盖已有数据

### 6.2 序列化格式

```cpp
struct SerializedMemIndex {
    // Header (64 bytes, cache line aligned)
    struct Header {
        uint32_t magic;              // 0x4D494458 ("MIDX")
        uint32_t version;
        uint64_t entry_count;        // 有效条目数
        uint64_t capacity;           // 哈希表容量
        uint64_t global_sequence;    // 全局序列号
        uint32_t checksum;           // 数据校验和
        uint8_t padding[28];
    } header;

    // 紧凑的条目数组 (只存储非空条目)
    struct CompactEntry {
        uint64_t key;
        uint32_t file_id      : 10;
        uint32_t offset_index : 21;
        uint32_t deleted      : 1;
        uint16_t tag          : 8;
        uint16_t page_count   : 8;
        uint16_t sequence;
    } entries[];  // entry_count个
};
```

### 6.2 序列化实现

```cpp
size_t MemIndex::serialize(void* buffer, size_t buf_size) {
    auto* output = static_cast<SerializedMemIndex*>(buffer);

    // 1. 填充header
    output->header.magic = 0x4D494458;
    output->header.version = 1;
    output->header.capacity = capacity_;
    output->header.global_sequence = global_sequence_;

    // 2. 遍历并复制非空条目
    uint64_t count = 0;
    for (uint64_t i = 0; i < capacity_; i++) {
        if (psl_[i] > 0 && !entries_[i].deleted) {
            memcpy(&output->entries[count], &entries_[i], sizeof(MemIndexEntry));
            count++;
        }
    }
    output->header.entry_count = count;

    // 3. 计算校验和
    size_t data_size = sizeof(SerializedMemIndex::Header) +
                       count * sizeof(SerializedMemIndex::CompactEntry);
    output->header.checksum = crc32(output->entries,
                                    count * sizeof(SerializedMemIndex::CompactEntry));

    return data_size;
}
```

### 6.3 启动加载索引 (详细流程)

**恢复流程概述**:

```
┌─────────────────────────────────────────────────────────────────────┐
│                         启动恢复流程                                  │
├─────────────────────────────────────────────────────────────────────┤
│  1. 加载 Superblock (包含 checkpoint 元信息)                          │
│     ↓                                                               │
│  2. 加载 MemIndex Area A/B，选择版本号更高的                           │
│     ↓                                                               │
│  3. 反序列化 MemIndex (内存镜像加载或逐条 upsert)                       │
│     ↓                                                               │
│  4. 增量恢复: 扫描 checkpoint 之后的数据记录                           │
│     - 起点: checkpoint_page_index (快照时刻的位置)                     │
│     - 终点: 各 Data Blob 的物理末尾 (非 Superblock 记录的快照点)        │
│     - 对每条记录：使用持久化的 sequence 调用 upsert                     │
│     - 记录 recovered_max_sequence                                    │
│     ↓                                                               │
│  5. 同步序列号分配器                                                   │
│     - new_global_seq = max(checkpoint_seq, recovered_max_seq) + 1   │
│     ↓                                                               │
│  6. 恢复完成，可以接受新的读写请求                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**关键设计: 扫描范围必须覆盖到物理末尾**

在执行增量 Checkpoint 过程中，`checkpoint_start_sequence_` 被采集并存入快照，但在异步写入 Superblock 成功之前，可能有大量新请求（如 Seq 2000-3000）已经写入了磁盘。

若恢复时仅从 `checkpoint_page_index` 扫描到 Superblock 记录的 `active_buffer_positions`，可能会漏掉这些已写盘但还没来得及包含在索引镜像里的记录。

**因此，扫描终点必须是各 Data Blob 的物理末尾**，而非 Superblock 中记录的快照点：

```cpp
// 确定扫描范围
struct ScanRange {
    uint16_t file_id;
    uint64_t start_page;   // 从 checkpoint_page_index 开始
    uint64_t end_page;     // 到 blob 的物理末尾 (通过 spdk_blob_get_num_clusters 获取)
};

// 获取每个数据文件的物理末尾
uint64_t get_blob_physical_end(spdk_blob* blob) {
    uint64_t num_clusters = spdk_blob_get_num_clusters(blob);
    uint64_t cluster_size = spdk_bs_get_cluster_size(blob_store_);
    return (num_clusters * cluster_size) / 4096;  // 转换为 page 数
}

// 构建扫描范围列表
std::vector<ScanRange> build_scan_ranges() {
    std::vector<ScanRange> ranges;

    for (auto& file : data_files_) {
        ScanRange range;
        range.file_id = file.file_id;

        // 起点: 从 checkpoint 位置开始
        if (file.file_id == superblock_.checkpoint_file_id) {
            range.start_page = superblock_.checkpoint_page_index;
        } else if (file.file_id > superblock_.checkpoint_file_id) {
            range.start_page = 0;  // checkpoint 之后创建的文件，从头扫描
        } else {
            continue;  // checkpoint 之前的文件已包含在索引镜像中
        }

        // 终点: blob 的物理末尾 (而非 Superblock 中的快照点)
        range.end_page = get_blob_physical_end(file.blob);

        ranges.push_back(range);
    }

    return ranges;
}
```

**一致性保证**:
- 恢复时使用持久化的 sequence 而非重新分配，保证同一 key 的多次写入顺序正确
- upsert 内部通过序列号比较，只有新值才会覆盖旧值
- 恢复完成后同步全局序列号，保证新写入一定获得更大的序列号
- **扫描到物理末尾**确保"日志重放"的完整性，不会漏掉已写盘的记录

```cpp
class IndexLoader {
public:
    // 异步加载状态机
    enum class State {
        INIT,
        LOADING_SUPERBLOCK,
        LOADING_MEM_INDEX_A,
        LOADING_MEM_INDEX_B,
        COMPARING_VERSIONS,
        DESERIALIZING,
        SCANNING_DATA_FILES,
        REBUILDING_INCREMENTAL,
        DONE,
        ERROR
    };

    void start_load(spdk_kv_cb cb, void* cb_arg) {
        callback_ = cb;
        cb_arg_ = cb_arg;
        state_ = State::LOADING_SUPERBLOCK;
        load_superblock();
    }

private:
    void load_superblock() {
        // 异步读取Primary Superblock
        spdk_blob_io_read(superblock_blob_, channel_,
                          read_buffer_, 0, 1,
                          on_superblock_loaded, this);
    }

    static void on_superblock_loaded(void* arg, int status) {
        auto* self = static_cast<IndexLoader*>(arg);

        if (status != 0) {
            // 尝试读取Backup
            self->load_backup_superblock();
            return;
        }

        if (!self->validate_superblock()) {
            self->load_backup_superblock();
            return;
        }

        // 开始加载MemIndex
        self->state_ = State::LOADING_MEM_INDEX_A;
        self->load_mem_index_area(0);
    }

    void load_mem_index_area(int area) {
        spdk_blob_id blob_id = (area == 0) ?
            superblock_.mem_index_blob_a : superblock_.mem_index_blob_b;

        // 计算需要读取的大小
        size_t read_size = superblock_.mem_index_size;

        // 分批异步读取 (每次读取2MB)
        current_read_offset_ = 0;
        current_area_ = area;
        read_next_chunk();
    }

    void read_next_chunk() {
        size_t remaining = superblock_.mem_index_size - current_read_offset_;
        size_t chunk_size = std::min(remaining, (size_t)(2 * 1024 * 1024));

        if (chunk_size == 0) {
            on_area_load_complete();
            return;
        }

        spdk_blob_io_read(mem_index_blobs_[current_area_], channel_,
                          mem_index_buffer_ + current_read_offset_,
                          current_read_offset_ / 4096,
                          chunk_size / 4096,
                          on_chunk_loaded, this);
    }

    static void on_chunk_loaded(void* arg, int status) {
        auto* self = static_cast<IndexLoader*>(arg);

        if (status != 0) {
            self->handle_load_error(status);
            return;
        }

        self->current_read_offset_ += 2 * 1024 * 1024;
        self->read_next_chunk();
    }

    void on_area_load_complete() {
        // 验证并解析header
        auto* header = reinterpret_cast<SerializedMemIndex::Header*>(
            mem_index_buffer_);

        if (header->magic != 0x4D494458) {
            mem_index_versions_[current_area_] = 0;  // 无效
        } else {
            mem_index_versions_[current_area_] = header->global_sequence;
            // 校验checksum
            if (!validate_checksum(header)) {
                mem_index_versions_[current_area_] = 0;
            }
        }

        if (current_area_ == 0) {
            // 继续加载Area B
            state_ = State::LOADING_MEM_INDEX_B;
            load_mem_index_area(1);
        } else {
            // 两个Area都加载完成，比较版本
            state_ = State::COMPARING_VERSIONS;
            compare_and_deserialize();
        }
    }

    void compare_and_deserialize() {
        // 选择版本更高的Area
        int best_area = (mem_index_versions_[0] >= mem_index_versions_[1]) ? 0 : 1;

        if (mem_index_versions_[best_area] == 0) {
            // 两个Area都无效，需要全量重建
            state_ = State::SCANNING_DATA_FILES;
            start_full_rebuild();
            return;
        }

        // 反序列化
        state_ = State::DESERIALIZING;
        deserialize_mem_index(best_area);
    }

    // =========== 优化: 内存镜像加载 (Memory Dump) ===========
    // 背景: 逐条 upsert 20 亿条记录，按 200MB/s 计算需要 100+ 秒
    // 优化: 直接将磁盘上的哈希表镜像 DMA 到内存，无需 rehash
    // 前提: 索引的 capacity 没有变化

    void deserialize_mem_index(int area) {
        auto* header = reinterpret_cast<SerializedMemIndex::Header*>(mem_index_buffer_);

        // 检查是否可以使用内存镜像加载
        if (can_use_memory_dump_load(header)) {
            load_as_memory_dump(area);
        } else {
            // 回退到逐条插入 (capacity 变化时)
            load_by_upsert(area);
        }
    }

    bool can_use_memory_dump_load(SerializedMemIndex::Header* header) {
        // 条件: capacity 没有变化，且使用相同的哈希函数和布局
        return header->capacity == mem_index_->capacity() &&
               header->version == CURRENT_INDEX_VERSION;
    }

    void load_as_memory_dump(int area) {
        // 内存镜像加载: 直接将磁盘数据映射到内存索引
        // 加载时间仅受限于磁盘 IO 带宽 (3GB/s NVMe 约 13 秒加载 40GB)

        auto* header = reinterpret_cast<SerializedMemIndex::Header*>(mem_index_buffer_);

        log_info("Using memory dump load: %lu entries, capacity %lu",
                 header->entry_count, header->capacity);

        // 1. 直接从各 Segment Blob 并行读取到 MemIndex 内存区域
        size_t entries_size = header->capacity * sizeof(MemIndexEntry);
        size_t segment_size = entries_size / MEM_INDEX_SEGMENT_COUNT;

        pending_segment_loads_ = MEM_INDEX_SEGMENT_COUNT;

        for (uint32_t seg = 0; seg < MEM_INDEX_SEGMENT_COUNT; seg++) {
            spdk_blob_id blob_id = segment_blobs_[area][seg];

            // 计算目标内存地址和源偏移
            void* target_addr = static_cast<char*>(mem_index_->entries_) +
                               seg * segment_size;

            // 异步并行读取各 Segment
            spdk_blob_io_read(blob_store_, channel_,
                              target_addr,
                              0,  // 从 blob 开头读取
                              segment_size / 4096,
                              on_segment_load_complete, this);
        }
    }

    static void on_segment_load_complete(void* arg, int status) {
        auto* self = static_cast<IndexLoader*>(arg);

        if (status != 0) {
            log_error("Segment load failed: %d", status);
            self->has_load_error_ = true;
        }

        if (--self->pending_segment_loads_ == 0) {
            // 所有 Segment 加载完成
            if (self->has_load_error_) {
                // 回退到逐条加载
                self->load_by_upsert(self->current_area_);
            } else {
                self->on_memory_dump_load_complete();
            }
        }
    }

    void on_memory_dump_load_complete() {
        auto* header = reinterpret_cast<SerializedMemIndex::Header*>(mem_index_buffer_);

        // 恢复 PSL 数组 (需要重新计算，因为 PSL 没有持久化)
        rebuild_psl_array();

        // 恢复全局序列号
        mem_index_->set_global_sequence(header->global_sequence);

        log_info("Memory dump load complete, starting incremental rebuild");

        // 继续增量恢复
        state_ = State::REBUILDING_INCREMENTAL;
        start_incremental_rebuild();
    }

    void rebuild_psl_array() {
        // 重新计算 PSL (Probe Sequence Length) 数组
        // 遍历所有非空 entry，计算其实际 PSL
        uint64_t capacity = mem_index_->capacity();
        MemIndexEntry* entries = mem_index_->entries_;
        uint8_t* psl = mem_index_->psl_;

        memset(psl, 0, capacity);

        for (uint64_t i = 0; i < capacity; i++) {
            if (entries[i].key != 0) {
                uint64_t hash;
                uint8_t tag;
                MemIndex::compute_hash(entries[i].key, &hash, &tag);
                uint64_t ideal_idx = hash & (capacity - 1);

                // 计算实际距离
                uint64_t distance = (i >= ideal_idx) ?
                                   (i - ideal_idx) :
                                   (capacity - ideal_idx + i);
                psl[i] = distance + 1;  // PSL 从 1 开始
            }
        }
    }

    // 回退方案: 逐条插入 (当 capacity 变化时使用)
    void load_by_upsert(int area) {
        auto* data = reinterpret_cast<SerializedMemIndex*>(mem_index_buffer_);

        log_info("Using upsert load: %lu entries (capacity changed)", data->header.entry_count);

        // 批量插入，使用批量 prefetch 优化
        constexpr size_t BATCH_SIZE = 64;
        uint64_t count = data->header.entry_count;

        for (uint64_t i = 0; i < count; i += BATCH_SIZE) {
            // Prefetch 下一批
            for (size_t j = 0; j < BATCH_SIZE && i + j + BATCH_SIZE < count; j++) {
                uint64_t key = data->entries[i + j + BATCH_SIZE].key;
                uint64_t hash = xxhash64(key);
                uint64_t idx = hash & (mem_index_->capacity() - 1);
                __builtin_prefetch(&mem_index_->entries_[idx], 1, 3);
            }

            // 插入当前批
            for (size_t j = 0; j < BATCH_SIZE && i + j < count; j++) {
                MemIndexEntry entry;
                memcpy(&entry, &data->entries[i + j], sizeof(MemIndexEntry));
                mem_index_->upsert(entry.key, entry);
            }
        }

        // 继续增量恢复
        state_ = State::REBUILDING_INCREMENTAL;
        start_incremental_rebuild();
    }

    void start_incremental_rebuild() {
        // 获取 checkpoint 之后的数据文件
        checkpoint_file_id_ = superblock_.checkpoint_file_id;
        checkpoint_page_index_ = superblock_.checkpoint_page_index;
        recovered_max_sequence_ = 0;

        // 构建需要扫描的文件列表 (包含所有活跃 Buffer 位置)
        build_files_to_scan();

        // 开始扫描
        current_file_idx_ = 0;
        scan_next_file();
    }

    void build_files_to_scan() {
        // 从 checkpoint 文件开始，到所有活跃 Buffer 所在文件
        files_to_scan_.clear();

        // 添加 checkpoint 文件
        if (checkpoint_file_id_ < superblock_.file_count) {
            files_to_scan_.push_back(file_mappings_[checkpoint_file_id_]);
        }

        // 添加所有活跃 Append Buffer 所在的文件
        for (uint8_t i = 0; i < superblock_.active_buffer_count; i++) {
            uint16_t file_id = superblock_.active_buffer_positions[i].file_id;
            // 避免重复添加
            bool exists = false;
            for (const auto& f : files_to_scan_) {
                if (f.file_id == file_id) {
                    exists = true;
                    break;
                }
            }
            if (!exists && file_id < superblock_.file_count) {
                files_to_scan_.push_back(file_mappings_[file_id]);
            }
        }

        // 按 file_id 排序
        std::sort(files_to_scan_.begin(), files_to_scan_.end(),
                  [](const FileInfo& a, const FileInfo& b) {
                      return a.file_id < b.file_id;
                  });
    }

    void scan_next_file() {
        if (current_file_idx_ >= files_to_scan_.size()) {
            // 完成增量恢复
            finalize_recovery();
            state_ = State::DONE;
            callback_(cb_arg_, KvError::SUCCESS);
            return;
        }

        auto& file = files_to_scan_[current_file_idx_];

        // 确定起始偏移 (使用 page index，直接用于 SPDK 读取)
        uint64_t start_page_index = sizeof(DataFileHeader) / 4096;
        if (file.file_id == checkpoint_file_id_) {
            start_page_index = checkpoint_page_index_;  // 已经是 page index，无需转换
        }

        current_scan_offset_ = start_page_index * 4096;  // 转为字节偏移用于内部计算
        scan_file_entries();
    }

    void scan_file_entries() {
        auto& file = files_to_scan_[current_file_idx_];

        if (current_scan_offset_ >= file.size) {
            // 当前文件扫描完成
            current_file_idx_++;
            scan_next_file();
            return;
        }

        // 读取一批entries
        size_t read_size = std::min((size_t)(1 * 1024 * 1024),
                                    file.size - current_scan_offset_);

        spdk_blob_io_read(file.blob, channel_,
                          scan_buffer_,
                          current_scan_offset_ / 4096,
                          read_size / 4096,
                          on_scan_chunk_loaded, this);
    }

    static void on_scan_chunk_loaded(void* arg, int status) {
        auto* self = static_cast<IndexLoader*>(arg);

        if (status != 0) {
            // 读取失败，跳过剩余部分
            self->current_file_idx_++;
            self->scan_next_file();
            return;
        }

        // 解析entries
        self->parse_and_rebuild_entries();
    }

    void parse_and_rebuild_entries() {
        size_t offset = 0;
        auto& file = files_to_scan_[current_file_idx_];

        while (offset < bytes_read_) {
            auto* header = reinterpret_cast<EntryHeader*>(scan_buffer_ + offset);

            // 验证 magic
            if (header->magic != ENTRY_MAGIC) {
                // Magic 无效可能是到达文件末尾或遇到空白区域
                // 跳到下一个 4KB 对齐位置继续尝试
                size_t next_aligned = ALIGN_UP(offset + 1, 4096);
                if (next_aligned >= bytes_read_) {
                    // 本批次扫描完成
                    break;
                }
                offset = next_aligned;
                continue;
            }

            // 解析 entry (注意：EntryHeader 现在是 16 bytes)
            uint64_t key = *reinterpret_cast<uint64_t*>(scan_buffer_ + offset + sizeof(EntryHeader));
            uint32_t value_len = *reinterpret_cast<uint32_t*>(
                scan_buffer_ + offset + sizeof(EntryHeader) + sizeof(uint64_t));
            size_t entry_size = ALIGN_UP(sizeof(EntryHeader) + 8 + 4 + value_len + 4, 4096);

            // 边界检查：确保 entry 完整在 buffer 内
            if (offset + entry_size > bytes_read_) {
                // Entry 跨越 buffer 边界，需要在下一批次处理
                break;
            }

            // 验证 checksum
            uint32_t stored_checksum = *reinterpret_cast<uint32_t*>(
                scan_buffer_ + offset + entry_size - 4);
            uint32_t computed_checksum = crc32(scan_buffer_ + offset, entry_size - 4);

            if (stored_checksum != computed_checksum) {
                // CRC 校验失败：Bitcask 是追加写，后续数据在逻辑上都不可信
                // 必须停止扫描此文件，避免加载损坏数据
                log_warning("CRC mismatch at file %u offset %lu, stopping scan",
                           file.file_id, current_scan_offset_ + offset);
                current_file_idx_++;
                scan_next_file();
                return;
            }

            // 更新索引 (使用持久化的 sequence 而非重新分配)
            MemIndexEntry new_entry;
            new_entry.key = key;
            new_entry.file_id = file.file_id;
            new_entry.offset_index = (current_scan_offset_ + offset) / 4096;
            new_entry.page_count = (entry_size + 4095) / 4096;
            new_entry.deleted = (header->flags & FLAG_DELETED) ? 1 : 0;
            new_entry.sequence = header->sequence;  // 使用持久化的序列号

            // 更新全局序列号计数器 (确保新写入的序列号大于所有已恢复的)
            if (header->sequence > recovered_max_sequence_) {
                recovered_max_sequence_ = header->sequence;
            }

            uint64_t hash;
            MemIndex::compute_hash(key, &hash, &new_entry.tag);

            if (new_entry.deleted) {
                mem_index_->remove(key);
            } else {
                mem_index_->upsert(key, new_entry);
            }

            offset += entry_size;
        }

        current_scan_offset_ += offset;  // 只增加实际处理的偏移
        scan_file_entries();
    }

    // =========== 关键: 恢复完成后同步序列号分配器 ===========
    //
    // **重要性**: 这一步是保证数据一致性的关键。如果不正确同步全局序列号，
    // 新写入的数据可能获得比已恢复数据更小的序列号，导致：
    // 1. 新值被旧值覆盖 (upsert 的序列号比较会认为旧值更新)
    // 2. 重启后数据顺序错乱
    //
    // **同步逻辑**:
    // 1. 从 Superblock 获取 checkpoint 时的 global_sequence
    // 2. 从增量恢复过程中记录 recovered_max_sequence (所有已扫描记录的最大序列号)
    // 3. 取二者的最大值 + 1 作为新的起始序列号
    //
    // **示例**:
    // - Checkpoint 时 global_seq = 1000
    // - 增量恢复发现 seq=1001, 1002, 1003 的记录
    // - recovered_max_sequence = 1003
    // - 新的 global_seq = max(1000, 1003) + 1 = 1004
    // - 后续写入将从 1004 开始分配序列号
    //
    void finalize_recovery() {
        // 从 Superblock 中获取 checkpoint 时的全局序列号
        uint32_t checkpoint_seq = superblock_.checkpoint_global_seq;

        // 新的全局序列号 = max(checkpoint_seq, recovered_max_sequence) + 1
        // 这保证新写入的序列号一定大于所有已恢复数据的序列号
        uint32_t new_global_seq = std::max(checkpoint_seq, recovered_max_sequence_) + 1;
        mem_index_->set_global_sequence(new_global_seq);

        log_info("Recovery complete: checkpoint_seq=%u, recovered_max_seq=%u, new_global_seq=%u",
                 checkpoint_seq, recovered_max_sequence_, new_global_seq);
    }

    State state_;
    spdk_kv_cb callback_;
    void* cb_arg_;
    Superblock superblock_;
    MemIndex* mem_index_;
    void* mem_index_buffer_;
    uint64_t mem_index_versions_[2];
    int current_area_;
    uint64_t current_read_offset_;
    std::vector<FileInfo> files_to_scan_;
    size_t current_file_idx_;
    uint64_t current_scan_offset_;
    uint16_t checkpoint_file_id_;
    uint64_t checkpoint_page_index_;    // 改为 page index (原 checkpoint_offset_)
    uint32_t recovered_max_sequence_;   // 恢复过程中发现的最大序列号
    void* scan_buffer_;
    size_t bytes_read_;
};
```

---

## 7. DMA内存池管理 (NUMA优化 + 1GB大页)

### 7.0 1GB 大页内存优化

**设计背景**:
- 40GB 索引表如果使用 4KB 页，需要约 1000 万个页表项，TLB Miss 会显著增加地址转换延迟
- 使用 1GB 大页后，40GB 仅需 40 个页表项，大幅减少 TLB Miss
- 在高 IOPS 场景下，TLB Miss 的代价可能占总延迟的 5-15%

**系统配置要求**:
```bash
# 在系统启动参数中配置 1GB 大页
# /etc/default/grub
GRUB_CMDLINE_LINUX="default_hugepagesz=1G hugepagesz=1G hugepages=64"

# 或运行时配置 (需要连续内存)
echo 64 > /sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages

# 挂载 hugetlbfs
mount -t hugetlbfs -o pagesize=1G none /mnt/hugepages-1G
```

**1GB 大页内存分配器 (优化预热策略)**:

**预热策略说明**:
- 同步 `memset` 40GB 内存会产生约 5-10 秒的延迟
- 优化方案:
  1. **异步预热**: 在 IndexLoader 读取数据的过程中自然触发物理页分配
  2. **延迟预热**: 仅在首次访问时触发，避免启动时的集中延迟
  3. **并行预热**: 使用多线程并行预热不同的内存区域

```cpp
class HugePageAllocator {
public:
    enum class PageSize {
        PAGE_4K   = 4096,
        PAGE_2M   = 2 * 1024 * 1024,
        PAGE_1G   = 1024 * 1024 * 1024
    };

    // 预热模式
    enum class WarmupMode {
        SYNC,           // 同步预热 (分配时立即 memset)
        ASYNC,          // 异步预热 (后台线程预热)
        LAZY,           // 延迟预热 (首次访问时触发)
        ON_LOAD         // 加载时预热 (IndexLoader 读取时自然触发)
    };

    // 分配 1GB 大页内存 (支持不同预热模式)
    static void* alloc_1g_hugepage(size_t size, int numa_node = -1,
                                   WarmupMode warmup = WarmupMode::ON_LOAD) {
        // 向上对齐到 1GB
        size_t aligned_size = (size + (1ULL << 30) - 1) & ~((1ULL << 30) - 1);

        void* ptr = nullptr;

        // 方法1: 使用 mmap + MAP_HUGETLB
        int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB;
        flags |= MAP_HUGE_1GB;  // 指定 1GB 大页

        ptr = mmap(nullptr, aligned_size, PROT_READ | PROT_WRITE, flags, -1, 0);

        if (ptr == MAP_FAILED) {
            // 回退到 2MB 大页
            flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_HUGE_2MB;
            ptr = mmap(nullptr, aligned_size, PROT_READ | PROT_WRITE, flags, -1, 0);
        }

        if (ptr == MAP_FAILED) {
            return nullptr;
        }

        // NUMA 绑定
        if (numa_node >= 0) {
            unsigned long nodemask = 1UL << numa_node;
            mbind(ptr, aligned_size, MPOL_BIND, &nodemask,
                  sizeof(nodemask) * 8, MPOL_MF_STRICT | MPOL_MF_MOVE);
        }

        // 根据预热模式处理
        switch (warmup) {
        case WarmupMode::SYNC:
            // 同步预热 (启动时阻塞)
            memset(ptr, 0, aligned_size);
            break;

        case WarmupMode::ASYNC:
            // 异步预热 (后台线程)
            start_async_warmup(ptr, aligned_size, numa_node);
            break;

        case WarmupMode::LAZY:
            // 延迟预热: 使用 madvise 提示内核
            madvise(ptr, aligned_size, MADV_WILLNEED);
            break;

        case WarmupMode::ON_LOAD:
            // 加载时预热: 不做任何操作，由 IndexLoader 读取时自然触发
            // 这是最优方案，因为读取数据本身就会写入内存，触发页分配
            break;
        }

        return ptr;
    }

private:
    // 异步预热实现
    static void start_async_warmup(void* ptr, size_t size, int numa_node) {
        // 启动后台线程进行预热
        std::thread warmup_thread([ptr, size, numa_node]() {
            // 绑定到目标 NUMA 节点
            if (numa_node >= 0) {
                cpu_set_t cpuset;
                CPU_ZERO(&cpuset);

                // 获取该 NUMA 节点上的 CPU 列表
                struct bitmask* cpus = numa_allocate_cpumask();
                numa_node_to_cpus(numa_node, cpus);

                // 选择该节点上的第一个 CPU
                for (int i = 0; i < numa_num_possible_cpus(); i++) {
                    if (numa_bitmask_isbitset(cpus, i)) {
                        CPU_SET(i, &cpuset);
                        break;
                    }
                }
                numa_free_cpumask(cpus);

                pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
            }

            // 并行预热: 按 1GB 块进行
            size_t chunk_size = 1ULL << 30;  // 1GB
            char* base = static_cast<char*>(ptr);

            for (size_t offset = 0; offset < size; offset += chunk_size) {
                size_t this_chunk = std::min(chunk_size, size - offset);
                memset(base + offset, 0, this_chunk);

                log_debug("Warmup progress: %zu / %zu MB",
                         (offset + this_chunk) / (1024 * 1024),
                         size / (1024 * 1024));
            }

            log_info("Async warmup completed: %zu MB", size / (1024 * 1024));
        });

        // detach 让线程在后台运行
        warmup_thread.detach();
    }

public:

    // 释放大页内存
    static void free_hugepage(void* ptr, size_t size) {
        size_t aligned_size = (size + (1ULL << 30) - 1) & ~((1ULL << 30) - 1);
        munmap(ptr, aligned_size);
    }

    // 使用 SPDK 的大页分配 (推荐)
    static void* alloc_spdk_hugepage(size_t size, int numa_node) {
        // SPDK 默认使用 2MB 大页，但可以配置使用 1GB
        // 需要在 SPDK 初始化时配置: --huge-page-size=1G

        void* ptr = spdk_dma_malloc_socket(size, 1ULL << 30,  // 1GB 对齐
                                           nullptr, numa_node);
        if (ptr) {
            // 预热
            memset(ptr, 0, size);
        }
        return ptr;
    }
};
```

**索引表分配策略**:
```cpp
class MemIndexAllocator {
public:
    // 为 80GB 索引表分配内存
    // 推荐使用 SYNC 模式，虽然启动慢几秒，但能保证运行时 0 Page Fault
    // 在 100 万 QPS 场景下，若使用 ON_LOAD，DMA 写入触发 1GB 大页 Page Fault
    // 会导致 CPU 核心产生毫秒级延迟抖动
    static void* allocate_index_memory(size_t size, int numa_node,
                                       HugePageAllocator::WarmupMode warmup =
                                           HugePageAllocator::WarmupMode::SYNC) {
        // 优先使用 1GB 大页
        void* ptr = HugePageAllocator::alloc_1g_hugepage(size, numa_node, warmup);

        if (ptr) {
            log_info("Allocated %zu bytes using 1GB hugepages (warmup: %d)",
                    size, static_cast<int>(warmup));
            return ptr;
        }

        // 回退到 SPDK 的 2MB 大页
        ptr = HugePageAllocator::alloc_spdk_hugepage(size, numa_node);

        if (ptr) {
            log_info("Allocated %zu bytes using 2MB hugepages (fallback)", size);
            return ptr;
        }

        // 最后回退到普通大页
        ptr = spdk_dma_malloc_socket(size, 4096, nullptr, numa_node);
        if (ptr) {
            log_warning("Allocated %zu bytes using 4KB pages (slow path)", size);
        }

        return ptr;
    }
};

// =========== 预热策略选择指南 ===========
//
// | 场景                    | 推荐模式   | 原因                                    |
// |------------------------|-----------|----------------------------------------|
// | 正常启动 (有 Checkpoint) | SYNC      | 保证运行时 0 Page Fault，延迟稳定        |
// | 全量重建                 | SYNC      | 保证运行时 0 Page Fault，延迟稳定        |
// | 测试/基准测试            | SYNC      | 需要稳定的性能数据，先完成预热再测试       |
// | 热备切换                 | ASYNC     | 后台预热，不影响切换速度                  |
//
// **性能风险说明**:
// 在 100 万 QPS 的场景下，如果使用 ON_LOAD 模式，SPDK 的 DMA 写入触发 1GB 大页
// Page Fault 会导致 CPU 核心产生显著的流水线停顿（Stall），产生毫秒级的延迟抖动。
// 因此对于追求极致稳定的高 QPS 系统，应将 SYNC 模式作为推荐方案。
// 虽然启动慢几秒，但能保证运行时 0 Page Fault。
//
// ON_LOAD 模式的工作原理 (仅用于理解，不推荐生产使用):
// 1. mmap 分配虚拟地址空间，但不分配物理页
// 2. IndexLoader 并行读取各 Segment，DMA 直接写入目标内存
// 3. 每次 DMA 写入时，内核自动分配物理页 (page fault)
// 4. 整个过程的预热开销被 IO 时间掩盖，用户无感知

// MemIndex 构造函数中使用大页分配
explicit MemIndex(uint64_t max_entries, double load_factor = 0.55, int numa_node = -1)
    : capacity_(next_power_of_2(static_cast<uint64_t>(max_entries / load_factor)))
    , size_(0)
    , global_sequence_(0)
    , numa_node_(numa_node) {

    size_t entries_size = capacity_ * sizeof(MemIndexEntry);
    size_t psl_size = capacity_;
    size_t total_size = entries_size + psl_size;

    // 使用 1GB 大页分配
    void* mem = MemIndexAllocator::allocate_index_memory(total_size, numa_node);
    if (!mem) {
        throw std::runtime_error("Failed to allocate index memory");
    }

    entries_ = static_cast<MemIndexEntry*>(mem);
    psl_ = static_cast<uint8_t*>(mem) + entries_size;

    // 内存已在分配时清零
}
```

**TLB 优化效果估算**:
| 配置 | 页表项数量 | TLB 命中率 (估算) |
|------|----------|------------------|
| 4KB 页 | ~1000万 | 60-70% |
| 2MB 页 | ~20000 | 90-95% |
| 1GB 页 | 40 | 99%+ |

### 7.1 NUMA亲和性设计

```cpp
class DmaMemoryPool {
public:
    DmaMemoryPool(size_t block_size, size_t block_count, int numa_node = -1)
        : block_size_(block_size)
        , block_count_(block_count)
        , numa_node_(numa_node)
        , mr_(nullptr) {

        // 1. 确定NUMA节点
        if (numa_node_ < 0) {
            numa_node_ = get_current_numa_node();
        }

        // 2. 在指定NUMA节点上分配Hugepage内存
        size_t total_size = block_size_ * block_count_;
        pool_base_ = allocate_numa_memory(total_size, numa_node_);

        if (!pool_base_) {
            throw std::runtime_error("Failed to allocate NUMA-local memory");
        }

        // 3. 初始化空闲列表
        free_list_.reserve(block_count_);
        for (size_t i = 0; i < block_count_; i++) {
            free_list_.push_back(static_cast<char*>(pool_base_) + i * block_size_);
        }
    }

    ~DmaMemoryPool() {
        if (mr_) {
            ibv_dereg_mr(mr_);
        }
        free_numa_memory(pool_base_, block_size_ * block_count_);
    }

    // 分配一个block
    void* alloc() {
        if (free_list_.empty()) {
            return nullptr;
        }
        void* ptr = free_list_.back();
        free_list_.pop_back();
        return ptr;
    }

    // 释放一个block
    void free(void* ptr) {
        free_list_.push_back(ptr);
    }

    // RDMA MR注册
    void register_mr(ibv_pd* pd) {
        if (mr_) return;

        mr_ = ibv_reg_mr(pd, pool_base_, block_size_ * block_count_,
                         IBV_ACCESS_LOCAL_WRITE |
                         IBV_ACCESS_REMOTE_WRITE |
                         IBV_ACCESS_REMOTE_READ);
        if (!mr_) {
            throw std::runtime_error("Failed to register MR");
        }
    }

    ibv_mr* get_mr() const { return mr_; }
    size_t block_size() const { return block_size_; }
    size_t available() const { return free_list_.size(); }
    int numa_node() const { return numa_node_; }

private:
    static int get_current_numa_node() {
        return numa_node_of_cpu(sched_getcpu());
    }

    static void* allocate_numa_memory(size_t size, int numa_node) {
        // 使用SPDK的DMA内存分配 (自带Hugepage支持)
        void* ptr = spdk_dma_malloc_socket(size, 4096, nullptr, numa_node);

        if (ptr) {
            // 使用mbind确保内存绑定到指定NUMA节点
            unsigned long nodemask = 1UL << numa_node;
            mbind(ptr, size, MPOL_BIND, &nodemask, sizeof(nodemask) * 8,
                  MPOL_MF_STRICT | MPOL_MF_MOVE);
        }

        return ptr;
    }

    static void free_numa_memory(void* ptr, size_t size) {
        spdk_dma_free(ptr);
    }

    void* pool_base_;
    size_t block_size_;
    size_t block_count_;
    int numa_node_;
    std::vector<void*> free_list_;
    ibv_mr* mr_;
};
```

### 7.2 CPU核心绑定

```cpp
class EngineThread {
public:
    static void pin_to_core(int core_id) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);

        int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
        if (rc != 0) {
            throw std::runtime_error("Failed to pin thread to core");
        }

        // 获取该core所在的NUMA节点
        int numa_node = numa_node_of_cpu(core_id);

        // 设置内存分配策略为本地NUMA节点
        numa_set_preferred(numa_node);
    }
};
```

### 7.3 预定义内存池 (NUMA感知)

```cpp
struct MemoryPools {
    int numa_node;

    // AppendBuffer使用 (2MB * 4, 多buffer轮换)
    DmaMemoryPool append_pool;

    // 读取buffer池 (64KB * 256)
    DmaMemoryPool read_pool;

    // 小buffer池，用于header读取等 (4KB * 1024)
    DmaMemoryPool small_pool;

    MemoryPools(int core_id)
        : numa_node(numa_node_of_cpu(core_id))
        , append_pool(2 * 1024 * 1024, 4, numa_node)
        , read_pool(64 * 1024, 256, numa_node)
        , small_pool(4 * 1024, 1024, numa_node) {}
};
```

### 7.4 RDMA集成流程（优化版）

**设计背景**:
- 在 SPDK 回调内直接触发 RDMA Send 会延长回调执行时间，影响 Polling 性能
- RDMA Send 过程中若 Buffer 被释放会导致数据污染
- 解决方案：回调内仅入队，主循环执行 RDMA；Buffer 引入引用计数

**优化后的流程**:
```
1. RDMA请求到达 (Polling)
2. 从read_pool分配buffer (NUMA本地内存)
3. 触发spdk_kv_get，buffer作为目标地址
4. SPDK IO完成 (Polling)
5. 在SPDK回调内部将任务放入rdma_send_queue (Lock-free Queue)
6. 主Polling循环在每轮末尾检查rdma_send_queue并执行RDMA提交
7. RDMA Send完成后，通过引用计数机制安全释放buffer
```

### 7.5 异步回调任务队列

```cpp
// 回调任务队列 (避免在IO回调中执行耗时操作)
class CallbackTaskQueue {
public:
    struct Task {
        enum class Type {
            RDMA_SEND,          // RDMA发送
            INDEX_UPDATE,       // 索引更新
            USER_CALLBACK       // 用户回调
        };

        Type type;
        void* context;
        void (*execute)(void* ctx);  // 执行函数
    };

    CallbackTaskQueue() {
        // 使用SPDK的无锁ring
        task_ring_ = spdk_ring_create(SPDK_RING_TYPE_SP_SC, 4096,
                                      SPDK_ENV_SOCKET_ID_ANY);
    }

    // 在IO回调中调用：将任务入队 (非阻塞)
    bool enqueue(Task task) {
        return spdk_ring_enqueue(task_ring_, (void**)&task, 1, nullptr) == 1;
    }

    // 在主Polling循环中调用：批量执行任务
    void process_tasks(size_t max_tasks = 64) {
        Task tasks[64];
        size_t count = spdk_ring_dequeue(task_ring_, (void**)tasks,
                                         std::min(max_tasks, (size_t)64));

        for (size_t i = 0; i < count; i++) {
            tasks[i].execute(tasks[i].context);
        }
    }

private:
    struct spdk_ring* task_ring_;
};

// SPDK IO完成回调 (优化版)
static void on_read_complete_optimized(void* arg, int status) {
    auto* ctx = static_cast<ReadContext*>(arg);

    if (status != 0) {
        // 错误处理入队
        ctx->engine->task_queue().enqueue({
            CallbackTaskQueue::Task::Type::USER_CALLBACK,
            ctx,
            [](void* c) {
                auto* ctx = static_cast<ReadContext*>(c);
                ctx->callback(ctx->cb_arg, KvError::IO_ERROR);
                ctx->engine->release_read_context(ctx);
            }
        });
        return;
    }

    // 将RDMA发送任务入队 (不在回调内直接执行)
    ctx->engine->task_queue().enqueue({
        CallbackTaskQueue::Task::Type::RDMA_SEND,
        ctx,
        [](void* c) {
            auto* ctx = static_cast<ReadContext*>(c);
            ctx->engine->rdma_manager()->send_async(ctx);
        }
    });
}
```

### 7.6 Buffer引用计数管理

**设计背景**:
- 如果 RDMA 还在 Send 过程中，SPDK 将 Buffer 释放回内存池并分配给下一个请求，会导致数据污染
- 解决方案：Buffer 引入引用计数，只有当 spdk_io_complete 和 rdma_send_complete 都发生后才真正释放

```cpp
// 带引用计数的DMA Buffer
class RefCountedBuffer {
public:
    RefCountedBuffer(void* data, size_t size, DmaMemoryPool* pool)
        : data_(data), size_(size), pool_(pool), ref_count_(1) {}

    // 增加引用
    void add_ref() {
        ref_count_.fetch_add(1, std::memory_order_relaxed);
    }

    // 减少引用，返回true表示已释放
    bool release() {
        if (ref_count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            // 最后一个引用，返回池中
            pool_->free(data_);
            delete this;
            return true;
        }
        return false;
    }

    void* data() { return data_; }
    size_t size() const { return size_; }
    int ref_count() const { return ref_count_.load(std::memory_order_relaxed); }

private:
    void* data_;
    size_t size_;
    DmaMemoryPool* pool_;
    std::atomic<int> ref_count_;
};

// 使用示例
struct ReadContext {
    RefCountedBuffer* buffer;
    // ... 其他字段
};

// SPDK读取完成
static void on_spdk_read_complete(void* arg, int status) {
    auto* ctx = static_cast<ReadContext*>(arg);
    // buffer引用: 初始1 (分配时)
    // 此时准备RDMA发送，增加引用
    ctx->buffer->add_ref();  // ref_count = 2

    // 入队RDMA发送任务
    enqueue_rdma_send(ctx);
}

// RDMA发送完成
static void on_rdma_send_complete(void* arg) {
    auto* ctx = static_cast<ReadContext*>(arg);
    // RDMA发送完成，释放引用
    ctx->buffer->release();  // ref_count = 1

    // 执行用户回调
    ctx->callback(ctx->cb_arg, KvError::SUCCESS);
}

// 用户回调执行后
static void cleanup_context(ReadContext* ctx) {
    // 释放最后一个引用，buffer返回池中
    ctx->buffer->release();  // ref_count = 0, buffer freed
    delete ctx;
}
```

### 7.7 完整的Polling循环

```cpp
void Engine::poll() {
    // 1. 处理前台IO完成 (高优先级)
    spdk_nvme_qpair_process_completions(foreground_qpair_, 32);

    // 2. 执行回调任务队列中的任务
    task_queue_.process_tasks(64);

    // 3. 处理AppendBuffer提交
    process_append_buffers();

    // 4. 处理等待队列
    process_wait_queue();

    // 5. 检查是否需要Checkpoint
    check_checkpoint_trigger();

    // 6. 处理Compaction (低优先级)
    if (!has_pending_foreground_io()) {
        compaction_scheduler_.poll();
        compaction_io_manager_->poll_completions();
    }

    // 7. 处理RDMA事件 (如果集成)
    rdma_manager_->poll();
}
```

### 7.8 RDMA 零拷贝读取接口

**设计背景**:
- 现有 `spdk_kv_get` 需要用户提供 `value_buf`，数据流: NVMe -> SPDK Buffer -> User Buffer -> RDMA
- 零拷贝模式下，直接使用 RDMA MR 注册的内存作为 SPDK 读取目标，数据流: NVMe -> RDMA MR -> NIC
- 全程无 CPU 拷贝，显著降低延迟和 CPU 占用

**零拷贝读取 API**:
```cpp
// 零拷贝读取选项
struct SpkvGetOpts {
    bool zero_copy;              // 启用零拷贝模式
    ibv_mr* rdma_mr;             // RDMA Memory Region (零拷贝模式必填)
    uint32_t rdma_rkey;          // Remote Key
    uint64_t rdma_remote_addr;   // 远端地址 (用于 RDMA Write)
};

// 扩展的 Get 接口 (支持零拷贝)
int spdk_kv_get_ex(spdk_kv_handle handle, uint64_t key,
                   void* value_buf, uint32_t buf_len,
                   SpkvGetOpts* opts,
                   spdk_kv_cb cb, void* cb_arg);

// RDMA 专用接口 (全程零拷贝)
int spdk_kv_get_rdma(spdk_kv_handle handle, uint64_t key,
                     ibv_mr* mr, uint64_t offset,
                     uint32_t max_len,
                     spdk_kv_rdma_cb cb, void* cb_arg);
```

**零拷贝实现**:
```cpp
struct ZeroCopyReadContext {
    uint64_t key;
    ibv_mr* mr;
    uint64_t mr_offset;
    uint32_t max_len;
    spdk_kv_rdma_cb callback;
    void* cb_arg;
    Engine* engine;

    // 实际读取的长度
    uint32_t actual_len;
};

void Engine::get_rdma_zerocopy(uint64_t key, ibv_mr* mr, uint64_t offset,
                               uint32_t max_len, spdk_kv_rdma_cb cb, void* cb_arg) {
    // 1. 查找索引
    MemIndexEntry* entry = mem_index_->find(key);
    if (!entry) {
        cb(cb_arg, KvError::KEY_NOT_FOUND, 0);
        return;
    }

    // 2. 计算读取位置
    FileInfo* file = get_file(entry->file_id);
    uint64_t offset_bytes = (uint64_t)entry->offset_index * 4096;
    uint32_t read_pages = entry->page_count;

    // 3. 直接使用 RDMA MR 作为目标地址 (零拷贝核心)
    void* target_addr = static_cast<char*>(mr->addr) + offset;

    // 4. 分配 context
    auto* ctx = new ZeroCopyReadContext{
        key, mr, offset, max_len, cb, cb_arg, this, 0
    };

    // 5. 发起 SPDK 读取，目标直接是 RDMA MR
    spdk_blob_io_read(file->blob, io_channel_,
                      target_addr,
                      offset_bytes / 4096,
                      read_pages,
                      on_zerocopy_read_complete, ctx);
}

static void on_zerocopy_read_complete(void* arg, int status) {
    auto* ctx = static_cast<ZeroCopyReadContext*>(arg);

    if (status != 0) {
        ctx->callback(ctx->cb_arg, KvError::IO_ERROR, 0);
        delete ctx;
        return;
    }

    // 解析 Entry Header 获取实际 value 长度
    void* data = static_cast<char*>(ctx->mr->addr) + ctx->mr_offset;
    auto* header = static_cast<EntryHeader*>(data);

    if (header->magic != ENTRY_MAGIC) {
        ctx->callback(ctx->cb_arg, KvError::CORRUPTION, 0);
        delete ctx;
        return;
    }

    // 获取 value 长度 (位于 header 之后的固定偏移)
    uint32_t value_len = *reinterpret_cast<uint32_t*>(
        static_cast<char*>(data) + sizeof(EntryHeader) + sizeof(uint64_t));

    ctx->actual_len = value_len;

    // 数据已在 RDMA MR 中，可以直接触发 RDMA Send/Write
    // 入队 RDMA 发送任务
    ctx->engine->task_queue().enqueue({
        CallbackTaskQueue::Task::Type::RDMA_SEND,
        ctx,
        [](void* c) {
            auto* ctx = static_cast<ZeroCopyReadContext*>(c);
            // 触发 RDMA 操作 (数据已就位)
            ctx->engine->rdma_manager()->send_from_mr(
                ctx->mr,
                ctx->mr_offset + sizeof(EntryHeader) + sizeof(uint64_t) + sizeof(uint32_t),
                ctx->actual_len,
                [](void* arg, int status) {
                    auto* ctx = static_cast<ZeroCopyReadContext*>(arg);
                    ctx->callback(ctx->cb_arg,
                                 status == 0 ? KvError::SUCCESS : KvError::IO_ERROR,
                                 ctx->actual_len);
                    delete ctx;
                },
                ctx
            );
        }
    });
}
```

**零拷贝数据流**:
```
传统模式:
NVMe SSD -> SPDK DMA Buffer -> memcpy -> User Buffer -> memcpy -> RDMA Send Buffer -> NIC

零拷贝模式:
NVMe SSD -> RDMA MR (直接DMA) -> NIC
            ^--- SPDK 读取目标直接是已注册的 RDMA 内存
```

**使用场景**:
- 适用于 RDMA 服务端，客户端请求 Get 后直接通过 RDMA 返回
- 要求 RDMA MR 必须是 4KB 对齐的 DMA 可访问内存
- value_buf 和 RDMA MR 必须足够大以容纳完整的 Entry (包括 header)

### 7.9 RDMA 场景下的 Buffer 生命周期锁定协议

**设计背景**:

在 4.4 节的 RDMA Put 流程中，客户端通过 `alloc_rdma_slot` 获取 AppendBuffer 中的一段地址，然后通过 RDMA WRITE 写入数据。这引入了一个关键问题：

1. **缓冲区管理权冲突**: AppendBufferManager 通过 spdk_ring 管理 Buffer 的分配、提交和归还。一旦 flush 完成，Buffer 会进入 `complete_ring_` 被 reset() 并重新分配。
2. **时序漏洞**: 如果客户端网络延迟较高，或 Backpressure 导致 Buffer 积压，AppendBufferManager 可能在 RDMA 传输完成前就执行 reset()，导致数据污染。

**解决方案: 引用计数 + 显式租约**

将 7.6 节的 `RefCountedBuffer` 机制扩展应用到 AppendBuffer 的 RDMA slot 管理中：

```cpp
// AppendBuffer 扩展：支持 RDMA slot 引用计数
class AppendBuffer {
public:
    // ... 现有成员 ...

    // RDMA slot 状态
    struct RdmaSlotState {
        bool is_allocated;         // 是否已分配
        bool rdma_write_complete;  // RDMA WRITE 是否完成
        uint32_t offset;           // slot 在 buffer 中的偏移
        uint32_t size;             // slot 大小
    };

    // 每个 buffer 可分配的最大 slot 数量
    static constexpr size_t MAX_SLOTS_PER_BUFFER = 64;
    RdmaSlotState rdma_slots_[MAX_SLOTS_PER_BUFFER];
    uint32_t active_rdma_slot_count_;  // 当前活跃的 RDMA slot 数量

    // 分配 RDMA slot 时增加引用计数
    void* alloc_rdma_slot(uint32_t size, uint32_t* out_slot_id) {
        void* slot = reserve_space(size);
        if (slot) {
            uint32_t slot_id = next_slot_id_++;
            rdma_slots_[slot_id] = {
                .is_allocated = true,
                .rdma_write_complete = false,
                .offset = get_offset(slot),
                .size = size
            };
            active_rdma_slot_count_++;
            *out_slot_id = slot_id;
        }
        return slot;
    }

    // 标记 RDMA WRITE 完成
    void mark_rdma_complete(uint32_t slot_id) {
        assert(slot_id < MAX_SLOTS_PER_BUFFER);
        rdma_slots_[slot_id].rdma_write_complete = true;
    }

    // 释放 RDMA slot
    void release_rdma_slot(uint32_t slot_id) {
        assert(slot_id < MAX_SLOTS_PER_BUFFER);
        rdma_slots_[slot_id].is_allocated = false;
        active_rdma_slot_count_--;
    }

    // 检查 buffer 是否可以安全 reset
    // 只有当所有 RDMA slot 都已释放时才能 reset
    bool can_reset() const {
        return active_rdma_slot_count_ == 0;
    }

    // 检查 buffer 是否可以提交到 SPDK
    // 只有当所有已分配 slot 的 RDMA WRITE 都完成时才能提交
    bool can_submit() const {
        for (uint32_t i = 0; i < MAX_SLOTS_PER_BUFFER; i++) {
            if (rdma_slots_[i].is_allocated && !rdma_slots_[i].rdma_write_complete) {
                return false;  // 还有 RDMA WRITE 未完成
            }
        }
        return true;
    }
};
```

**修改后的 AppendBufferManager::return_buffer 逻辑**:

```cpp
void AppendBufferManager::return_buffer(AppendBuffer* buffer) {
    // 不能立即 reset，需要检查 RDMA slot 状态
    if (buffer->can_reset()) {
        // 所有 RDMA 操作已完成，可以安全 reset
        buffer->reset();
        free_ring_.enqueue(buffer);
    } else {
        // 还有 RDMA slot 未释放，加入挂起队列
        pending_reset_queue_.push(buffer);
    }
}

// 在 polling 循环中检查挂起的 buffer
void AppendBufferManager::check_pending_resets() {
    AppendBuffer* buffer;
    while (pending_reset_queue_.try_pop(&buffer)) {
        if (buffer->can_reset()) {
            buffer->reset();
            free_ring_.enqueue(buffer);
        } else {
            // 仍然不能 reset，放回队列
            pending_reset_queue_.push(buffer);
            break;  // 避免死循环
        }
    }
}
```

**修改后的 RDMA Put 完整流程**:

```
1. 分配阶段 (alloc_rdma_slot):
   - Client 请求 slot
   - Engine 从 active_buffer 预留空间
   - 标记该 slot 为"已分配"，active_rdma_slot_count_++
   - 返回 slot 地址和 rkey

2. 传输阶段 (RDMA WRITE):
   - Client 通过 RDMA WRITE 将数据写入 slot
   - 此阶段 Engine 不感知，Buffer 处于"锁定"状态

3. 通知阶段 (rdma_write_complete):
   - Client 发送 RPC 通知 Engine RDMA WRITE 完成
   - Engine 调用 buffer->mark_rdma_complete(slot_id)

4. 提交阶段 (put_rdma):
   - Client 发送 put_rdma 请求
   - Engine 检查 slot 的 rdma_write_complete 标记
   - 如果未完成，返回错误 (防止数据损坏)
   - 如果已完成，构建 Header/Checksum，加入提交队列

5. 刷盘阶段 (flush):
   - IoSubmitter 在批量提交前检查 buffer->can_submit()
   - 如果有未完成的 RDMA slot，触发 Backpressure 暂停接收新请求
   - 所有 slot 的 RDMA WRITE 完成后，提交到 SPDK

6. 完成阶段 (on_io_complete):
   - SPDK IO 完成回调
   - 对每个 slot 调用 buffer->release_rdma_slot()
   - 检查 buffer->can_reset()，决定是否可以回收
```

**Backpressure 与 RDMA 的协调**:

```cpp
void IoSubmitter::submit_batch() {
    for (auto* buffer : pending_buffers_) {
        if (!buffer->can_submit()) {
            // 还有 RDMA WRITE 未完成，不能提交
            // 触发 Backpressure，暂停分配新的 RDMA slot
            buffer_manager_->activate_backpressure();
            return;
        }
    }

    // 所有 buffer 都可以提交
    for (auto* buffer : pending_buffers_) {
        spdk_blob_io_write(...);
    }
}
```

**关键保证**:

| 场景 | 保护机制 | 结果 |
|------|---------|------|
| RDMA WRITE 未完成就调用 put_rdma | rdma_write_complete 检查 | 返回错误，拒绝提交 |
| Buffer 在 RDMA WRITE 期间被提交 | can_submit() 检查 | 阻塞提交，等待 RDMA 完成 |
| IO 完成后 Buffer 被复用，但 RDMA 还在进行 | 不可能发生 | RDMA 必须在 IO 提交前完成 |
| IO 完成后立即 reset，但有 slot 未释放 | can_reset() + active_rdma_slot_count_ | 延迟 reset 直到所有 slot 释放 |

---

## 8. 空间回收 (Compaction) - 详细设计

### 8.0 Compaction IO隔离设计

**设计背景**:
- Compaction 会读取大量旧数据，可能干扰正常业务 IO
- 需要使用独立的 io_channel 实现 IO 隔离
- 利用 NVMe 背景优先级标记降低 Compaction 对前台业务的影响

```cpp
class CompactionIoManager {
public:
    CompactionIoManager(spdk_nvme_ctrlr* ctrlr, spdk_nvme_ns* ns)
        : ctrlr_(ctrlr), ns_(ns) {
        // 创建专用于Compaction的io_channel
        // 使用较低优先级的qpair
        struct spdk_nvme_io_qpair_opts opts;
        spdk_nvme_ctrlr_get_default_io_qpair_opts(ctrlr, &opts, sizeof(opts));

        // 设置较低的队列深度，限制Compaction的并发IO
        opts.io_queue_size = 64;  // 前台可能是256或更高

        // 如果硬件支持WRR (Weighted Round Robin)，设置较低权重
        // opts.qprio = SPDK_NVME_QPRIO_LOW;  // 需要检查硬件支持

        compaction_qpair_ = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, &opts, sizeof(opts));

        // 获取对应的io_channel
        compaction_channel_ = spdk_bdev_get_io_channel(bdev_desc_);
    }

    ~CompactionIoManager() {
        if (compaction_channel_) {
            spdk_put_io_channel(compaction_channel_);
        }
        if (compaction_qpair_) {
            spdk_nvme_ctrlr_free_io_qpair(compaction_qpair_);
        }
    }

    // Compaction读取使用的IO提交函数
    void read_async(spdk_blob* blob, void* buf, uint64_t offset_pages,
                   uint64_t num_pages, spdk_blob_op_complete cb, void* cb_arg) {
        // 使用专用channel
        spdk_blob_io_read(blob, compaction_channel_, buf,
                         offset_pages, num_pages, cb, cb_arg);
    }

    // Compaction写入使用的IO提交函数
    void write_async(spdk_blob* blob, void* buf, uint64_t offset_pages,
                    uint64_t num_pages, spdk_blob_op_complete cb, void* cb_arg) {
        // 可选：设置NVMe IO Flags降低优先级 (如果硬件支持)
        // spdk_nvme_ns_cmd_write_with_md(...)  // 带有特殊flags

        spdk_blob_io_write(blob, compaction_channel_, buf,
                          offset_pages, num_pages, cb, cb_arg);
    }

    // 专用的completion处理
    void poll_completions() {
        spdk_nvme_qpair_process_completions(compaction_qpair_, 0);
    }

    spdk_io_channel* channel() { return compaction_channel_; }

private:
    spdk_nvme_ctrlr* ctrlr_;
    spdk_nvme_ns* ns_;
    spdk_nvme_qpair* compaction_qpair_;
    spdk_io_channel* compaction_channel_;
    spdk_bdev_desc* bdev_desc_;
};
```

**IO优先级策略**:
```cpp
// 在poll循环中，优先处理前台IO，再处理Compaction IO
void Engine::poll() {
    // 1. 优先处理前台业务IO (高优先级)
    size_t foreground_completions = spdk_nvme_qpair_process_completions(
        foreground_qpair_, 32);

    // 2. 只有在前台IO不繁忙时才处理Compaction
    if (foreground_completions < 16) {
        // Compaction IO (低优先级)
        compaction_io_manager_->poll_completions();
    }

    // 3. 继续其他处理...
}
```

### 8.1 垃圾识别与位图管理（优化版）

**设计背景**:
- 原设计每文件 256KB 位图 (8GB/4KB/8)，1024 个文件共 256MB
- 大部分文件可能只有部分区域被使用，存在浪费
- 采用稀疏位图或按需维护策略降低内存占用

**方案一：稀疏位图 (使用 Roaring Bitmap 思想)**

```cpp
// 基于分块的稀疏位图
class SparseBitmap {
public:
    static constexpr size_t CHUNK_SIZE = 4096;  // 每chunk覆盖4096个bit (512字节)
    static constexpr size_t BITS_PER_CHUNK = CHUNK_SIZE * 8;

    SparseBitmap(size_t total_bits)
        : total_bits_(total_bits)
        , chunk_count_((total_bits + BITS_PER_CHUNK - 1) / BITS_PER_CHUNK) {}

    void set(size_t idx) {
        size_t chunk_idx = idx / BITS_PER_CHUNK;
        size_t bit_idx = idx % BITS_PER_CHUNK;

        auto it = chunks_.find(chunk_idx);
        if (it == chunks_.end()) {
            // 懒分配chunk
            chunks_[chunk_idx] = std::make_unique<Chunk>();
        }
        chunks_[chunk_idx]->set(bit_idx);
    }

    void clear(size_t idx) {
        size_t chunk_idx = idx / BITS_PER_CHUNK;
        size_t bit_idx = idx % BITS_PER_CHUNK;

        auto it = chunks_.find(chunk_idx);
        if (it != chunks_.end()) {
            it->second->clear(bit_idx);
            // 可选: 检查chunk是否全空，释放内存
            if (it->second->empty()) {
                chunks_.erase(it);
            }
        }
    }

    bool test(size_t idx) const {
        size_t chunk_idx = idx / BITS_PER_CHUNK;
        auto it = chunks_.find(chunk_idx);
        if (it == chunks_.end()) {
            return false;
        }
        return it->second->test(idx % BITS_PER_CHUNK);
    }

    // 内存使用量
    size_t memory_usage() const {
        return chunks_.size() * sizeof(Chunk) + sizeof(*this);
    }

private:
    struct Chunk {
        uint64_t data[BITS_PER_CHUNK / 64] = {0};

        void set(size_t idx) { data[idx / 64] |= (1ULL << (idx % 64)); }
        void clear(size_t idx) { data[idx / 64] &= ~(1ULL << (idx % 64)); }
        bool test(size_t idx) const { return data[idx / 64] & (1ULL << (idx % 64)); }
        bool empty() const {
            for (auto d : data) if (d) return false;
            return true;
        }
    };

    size_t total_bits_;
    size_t chunk_count_;
    std::unordered_map<size_t, std::unique_ptr<Chunk>> chunks_;
};
```

**方案二：按需维护位图 (仅对高垃圾率文件)**

```cpp
struct FileMetadata {
    uint16_t file_id;
    FileState state;
    uint64_t total_entries;
    uint64_t valid_entries;
    uint64_t total_bytes;
    uint64_t valid_bytes;

    // 仅当垃圾率超过阈值时才创建详细位图
    static constexpr double BITMAP_CREATION_THRESHOLD = 0.3;  // 垃圾率30%

    // 懒加载的位图 (nullptr表示不需要或未创建)
    std::unique_ptr<SparseBitmap> valid_bitmap;

    double garbage_ratio() const {
        if (total_bytes == 0) return 0.0;
        return 1.0 - (double)valid_bytes / total_bytes;
    }

    // 检查是否需要创建详细位图
    void maybe_create_bitmap() {
        if (!valid_bitmap && garbage_ratio() >= BITMAP_CREATION_THRESHOLD) {
            // 垃圾率超过阈值，创建稀疏位图
            size_t total_pages = 8ULL * 1024 * 1024 * 1024 / 4096;  // 8GB / 4KB
            valid_bitmap = std::make_unique<SparseBitmap>(total_pages);
        }
    }

    void mark_valid(uint32_t offset_index, uint16_t page_count, uint32_t bytes) {
        if (valid_bitmap) {
            for (uint16_t i = 0; i < page_count; i++) {
                valid_bitmap->set(offset_index + i);
            }
        }
        valid_entries++;
        valid_bytes += bytes;
    }

    void mark_invalid(uint32_t offset_index, uint16_t page_count, uint32_t bytes) {
        // 检查是否需要创建位图
        maybe_create_bitmap();

        if (valid_bitmap) {
            for (uint16_t i = 0; i < page_count; i++) {
                valid_bitmap->clear(offset_index + i);
            }
        }
        valid_entries--;
        valid_bytes -= bytes;
    }

    // 检查特定页面是否有效 (用于Compaction)
    bool is_page_valid(uint32_t offset_index) const {
        if (!valid_bitmap) {
            // 没有详细位图，通过其他方式判断 (如检查内存索引)
            return true;  // 保守策略：假设有效
        }
        return valid_bitmap->test(offset_index);
    }

    // 内存使用量统计
    size_t bitmap_memory_usage() const {
        return valid_bitmap ? valid_bitmap->memory_usage() : 0;
    }
};
```

**内存优化效果**:
- 原方案：每文件 256KB × 1024 文件 = 256MB
- 稀疏位图：只有实际使用的区域分配内存，预计节省 50-80%
- 按需维护：只对高垃圾率文件创建位图，大部分文件无位图开销

### 8.2 Compaction调度器 (精细优先级控制)

```cpp
class CompactionScheduler {
public:
    // 配置
    static constexpr size_t UNIT_SIZE = 1 * 1024 * 1024;  // 1MB per unit
    static constexpr uint64_t MAX_CYCLES_PER_POLL = 50000;  // 约25us @2GHz
    static constexpr uint32_t MAX_IOPS_PER_SEC = 1000;

    // 优先级控制阈值 (优化: 细化到积压数量而非简单有无判断)
    static constexpr size_t PAUSE_THRESHOLD = 1000;      // 积压超过1000请求时暂停
    static constexpr size_t RESUME_THRESHOLD = 100;      // 积压降至100以下恢复
    static constexpr size_t THROTTLE_THRESHOLD = 500;    // 积压超过500时限速

    CompactionScheduler(Engine* engine)
        : engine_(engine)
        , rate_limiter_(MAX_IOPS_PER_SEC)
        , compaction_paused_(false) {}

    void schedule_compaction(FileInfo* file) {
        auto task = std::make_unique<CompactionTask>(file, this);
        pending_tasks_.push(std::move(task));
    }

    // 在主polling loop中调用 (优化: 精细化优先级检查)
    void poll() {
        size_t pending_requests = engine_->pending_foreground_count();

        // 1. 精细化优先级控制 (避免低负载时Compaction完全不工作)
        if (compaction_paused_) {
            // 当前已暂停，检查是否可以恢复
            if (pending_requests <= RESUME_THRESHOLD) {
                compaction_paused_ = false;
                log_debug("Compaction resumed, pending requests: %zu", pending_requests);
            } else {
                return;  // 继续暂停
            }
        } else {
            // 检查是否需要暂停
            if (pending_requests >= PAUSE_THRESHOLD) {
                compaction_paused_ = true;
                log_debug("Compaction paused, pending requests: %zu", pending_requests);
                return;
            }

            // 中等负载时限速 (降低Compaction的IO带宽)
            if (pending_requests >= THROTTLE_THRESHOLD) {
                rate_limiter_.set_rate(MAX_IOPS_PER_SEC / 4);  // 降至25%
            } else if (pending_requests >= RESUME_THRESHOLD) {
                rate_limiter_.set_rate(MAX_IOPS_PER_SEC / 2);  // 降至50%
            } else {
                rate_limiter_.set_rate(MAX_IOPS_PER_SEC);      // 全速
            }
        }

        // 2. 限流检查
        if (!rate_limiter_.allow()) {
            return;
        }

        // 3. 没有任务
        if (pending_tasks_.empty() && !active_task_) {
            return;
        }

        // 4. 记录开始时间 (rdtsc)
        uint64_t start_cycles = rdtsc();

        // 5. 获取或继续任务
        if (!active_task_ && !pending_tasks_.empty()) {
            active_task_ = std::move(pending_tasks_.front());
            pending_tasks_.pop();
        }

        // 6. 执行任务，但限制CPU时间
        while (active_task_ && !active_task_->is_complete()) {
            // 检查是否有新的前台请求到达
            if (engine_->has_pending_foreground_requests()) {
                break;  // 立即暂停
            }

            // 检查CPU时间片
            uint64_t elapsed = rdtsc() - start_cycles;
            if (elapsed > MAX_CYCLES_PER_POLL) {
                break;  // 时间片用尽
            }

            // 执行一个步骤
            active_task_->step();
        }

        // 7. 任务完成处理
        if (active_task_ && active_task_->is_complete()) {
            active_task_.reset();
        }
    }

private:
    static inline uint64_t rdtsc() {
        uint32_t lo, hi;
        __asm__ volatile ("rdtsc" : "=a" (lo), "=d" (hi));
        return ((uint64_t)hi << 32) | lo;
    }

    Engine* engine_;
    RateLimiter rate_limiter_;
    std::queue<std::unique_ptr<CompactionTask>> pending_tasks_;
    std::unique_ptr<CompactionTask> active_task_;
    bool compaction_paused_;  // 暂停标志
};
```

### 8.3 Compaction任务实现 (带重试和回滚)

**设计背景**:
- 原设计中 IO 失败后直接进入 DONE 状态，可能导致数据丢失
- 源文件被标记为 DELETED，但数据未成功迁移到新文件
- 解决方案：增加重试机制 + 失败回滚

```cpp
class CompactionTask {
public:
    enum class State {
        INIT,
        MARK_COMPACTING,
        WAIT_MARK_COMPLETE,
        READ_CHUNK,
        WAIT_READ_COMPLETE,
        PROCESS_ENTRIES,
        WRITE_CHUNK,
        WAIT_WRITE_COMPLETE,
        UPDATE_INDICES,
        FINALIZE,
        MARK_DELETED,
        WAIT_DELETE_COMPLETE,
        // 新增: 失败处理状态
        RETRY_WAIT,           // 等待重试
        ROLLBACK,             // 回滚状态
        ROLLBACK_MARK_SEALED, // 回滚: 将源文件改回SEALED
        WAIT_ROLLBACK_COMPLETE,
        FAILED,               // 最终失败状态
        DONE
    };

    // 重试配置
    static constexpr int MAX_RETRY_COUNT = 3;
    static constexpr uint64_t RETRY_DELAY_US = 1000;  // 1ms

    CompactionTask(FileInfo* src_file, CompactionScheduler* scheduler)
        : src_file_(src_file)
        , scheduler_(scheduler)
        , state_(State::INIT)
        , current_offset_(sizeof(DataFileHeader))
        , dest_offset_(0)
        , write_buffer_used_(0)
        , io_pending_(false) {}

    // 单步执行 (非阻塞)
    void step() {
        // 如果有IO在进行，不执行新操作
        if (io_pending_) return;

        switch (state_) {
        case State::INIT:
            init_compaction();
            break;

        case State::MARK_COMPACTING:
            mark_file_compacting();
            break;

        case State::WAIT_MARK_COMPLETE:
            // IO回调会推进状态
            break;

        case State::READ_CHUNK:
            read_next_chunk();
            break;

        case State::WAIT_READ_COMPLETE:
            break;

        case State::PROCESS_ENTRIES:
            process_entries();
            break;

        case State::WRITE_CHUNK:
            write_chunk();
            break;

        case State::WAIT_WRITE_COMPLETE:
            break;

        case State::UPDATE_INDICES:
            update_indices();
            break;

        case State::FINALIZE:
            finalize();
            break;

        case State::MARK_DELETED:
            mark_file_deleted();
            break;

        case State::WAIT_DELETE_COMPLETE:
            break;

        // 新增: 重试等待状态
        case State::RETRY_WAIT:
            check_retry_timeout();
            break;

        // 新增: 回滚状态
        case State::ROLLBACK:
            start_rollback();
            break;

        case State::ROLLBACK_MARK_SEALED:
            rollback_mark_sealed();
            break;

        case State::WAIT_ROLLBACK_COMPLETE:
            break;

        case State::FAILED:
            // 记录失败信息，通知调度器
            break;

        case State::DONE:
            break;
        }
    }

    bool is_complete() const {
        return state_ == State::DONE || state_ == State::FAILED;
    }

    bool is_failed() const { return state_ == State::FAILED; }
    int last_error() const { return last_error_; }

private:
    void init_compaction() {
        // 分配目标文件
        dest_file_ = scheduler_->engine_->allocate_data_file();

        // 分配读写buffer
        read_buffer_ = scheduler_->engine_->pools().small_pool.alloc();
        write_buffer_ = scheduler_->engine_->pools().small_pool.alloc();

        state_ = State::MARK_COMPACTING;
    }

    void mark_file_compacting() {
        src_file_->state = FileState::COMPACTING;
        io_pending_ = true;

        // 异步更新文件头
        update_file_header_async(src_file_, [this](int status) {
            io_pending_ = false;
            if (status == 0) {
                state_ = State::READ_CHUNK;
            } else {
                state_ = State::DONE;  // 失败
            }
        });
    }

    void read_next_chunk() {
        // 使用位图快速跳过无效区域
        skip_invalid_pages();

        if (current_offset_ >= src_file_->size) {
            // 所有数据处理完成
            if (write_buffer_used_ > 0) {
                state_ = State::WRITE_CHUNK;
            } else {
                state_ = State::FINALIZE;
            }
            return;
        }

        size_t read_size = std::min((size_t)CHUNK_SIZE,
                                    src_file_->size - current_offset_);

        io_pending_ = true;
        spdk_blob_io_read(src_file_->blob, io_channel_,
                          read_buffer_, current_offset_ / 4096,
                          read_size / 4096,
                          on_read_complete, this);
    }

    void skip_invalid_pages() {
        auto& bitmap = src_file_->metadata.valid_bitmap;
        while (current_offset_ < src_file_->size) {
            uint32_t page_idx = current_offset_ / 4096;
            if (bitmap[page_idx / 64] & (1ULL << (page_idx % 64))) {
                break;  // 找到有效页
            }
            current_offset_ += 4096;
        }
    }

    static void on_read_complete(void* arg, int status) {
        auto* self = static_cast<CompactionTask*>(arg);
        self->io_pending_ = false;

        if (status == 0) {
            self->bytes_read_ = CHUNK_SIZE;
            self->retry_count_ = 0;  // 重置重试计数
            self->state_ = State::PROCESS_ENTRIES;
        } else {
            // 读取失败，尝试重试
            self->handle_io_error(State::READ_CHUNK, status);
        }
    }

    // 统一的IO错误处理
    void handle_io_error(State retry_state, int error_code) {
        retry_count_++;
        last_error_ = error_code;

        if (is_retryable_error(error_code) && retry_count_ < MAX_RETRY_COUNT) {
            // 可重试错误，等待后重试
            retry_target_state_ = retry_state;
            retry_timestamp_ = get_current_time_us() + RETRY_DELAY_US;
            state_ = State::RETRY_WAIT;
            log_warning("Compaction IO error %d, retry %d/%d",
                       error_code, retry_count_, MAX_RETRY_COUNT);
        } else {
            // 不可重试或重试次数用尽，进入回滚
            log_error("Compaction failed after %d retries, error: %d, rolling back",
                     retry_count_, error_code);
            state_ = State::ROLLBACK;
        }
    }

    // 判断是否为可重试错误
    static bool is_retryable_error(int error_code) {
        // 可重试: 暂时性拥塞、资源不足等
        switch (error_code) {
            case -EAGAIN:
            case -EBUSY:
            case -ENOMEM:
                return true;
            default:
                return false;
        }
    }

    void process_entries() {
        size_t offset = 0;

        while (offset < bytes_read_) {
            auto* header = reinterpret_cast<EntryHeader*>(
                static_cast<char*>(read_buffer_) + offset);

            // 验证magic
            if (header->magic != ENTRY_MAGIC) {
                break;
            }

            // 解析entry
            uint64_t key = *reinterpret_cast<uint64_t*>(
                static_cast<char*>(read_buffer_) + offset + 8);

            // 计算entry大小
            uint32_t value_len = *reinterpret_cast<uint32_t*>(
                static_cast<char*>(read_buffer_) + offset + 16);
            size_t entry_size = ALIGN_UP(8 + 8 + 4 + value_len + 4, 4096);

            // 二次确认: 检查内存索引
            auto* index_entry = scheduler_->engine_->mem_index()->find(key);

            bool is_valid = index_entry &&
                index_entry->file_id == src_file_->file_id &&
                index_entry->offset_index == (current_offset_ + offset) / 4096;

            if (is_valid) {
                // 复制到写buffer
                if (write_buffer_used_ + entry_size > CHUNK_SIZE) {
                    // 写buffer满，需要先写出
                    process_offset_ = offset;
                    state_ = State::WRITE_CHUNK;
                    return;
                }

                memcpy(static_cast<char*>(write_buffer_) + write_buffer_used_,
                       static_cast<char*>(read_buffer_) + offset,
                       entry_size);

                // 记录待更新的索引
                pending_updates_.push_back({
                    key,
                    src_file_->file_id,
                    (uint32_t)((current_offset_ + offset) / 4096),
                    dest_file_->file_id,
                    (uint32_t)((dest_offset_ + write_buffer_used_) / 4096),
                    (uint16_t)((entry_size + 4095) / 4096)
                });

                write_buffer_used_ += entry_size;
            }

            offset += entry_size;
        }

        current_offset_ += bytes_read_;
        state_ = State::READ_CHUNK;
    }

    void write_chunk() {
        if (write_buffer_used_ == 0) {
            state_ = State::READ_CHUNK;
            return;
        }

        io_pending_ = true;
        spdk_blob_io_write(dest_file_->blob, io_channel_,
                           write_buffer_, dest_offset_ / 4096,
                           write_buffer_used_ / 4096,
                           on_write_complete, this);
    }

    static void on_write_complete(void* arg, int status) {
        auto* self = static_cast<CompactionTask*>(arg);
        self->io_pending_ = false;

        if (status == 0) {
            self->retry_count_ = 0;  // 重置重试计数
            self->state_ = State::UPDATE_INDICES;
        } else {
            // 写入失败，尝试重试
            self->handle_io_error(State::WRITE_CHUNK, status);
        }
    }

    void update_indices() {
        // 更新所有待更新的索引 (Compaction优先级)
        for (auto& update : pending_updates_) {
            auto* existing = scheduler_->engine_->mem_index()->find(update.key);

            // 仅当索引仍指向旧位置时才更新
            if (existing &&
                existing->file_id == update.old_file_id &&
                existing->offset_index == update.old_offset_index) {

                MemIndexEntry new_entry = *existing;
                new_entry.file_id = update.new_file_id;
                new_entry.offset_index = update.new_offset_index;
                new_entry.page_count = update.page_count;

                scheduler_->engine_->mem_index()->upsert(update.key, new_entry);
            }
        }

        pending_updates_.clear();
        dest_offset_ += write_buffer_used_;
        write_buffer_used_ = 0;

        // 继续处理或读取下一块
        if (state_ == State::UPDATE_INDICES) {
            state_ = State::READ_CHUNK;
        }
    }

    void finalize() {
        // 写出剩余数据
        if (write_buffer_used_ > 0) {
            state_ = State::WRITE_CHUNK;
            return;
        }

        state_ = State::MARK_DELETED;
    }

    void mark_file_deleted() {
        src_file_->state = FileState::DELETED;
        io_pending_ = true;

        update_file_header_async(src_file_, [this](int status) {
            io_pending_ = false;
            // 释放buffer
            scheduler_->engine_->pools().small_pool.free(read_buffer_);
            scheduler_->engine_->pools().small_pool.free(write_buffer_);

            state_ = State::DONE;
        });
    }

    // === 新增: 重试和回滚相关函数 ===

    void check_retry_timeout() {
        if (get_current_time_us() >= retry_timestamp_) {
            // 重试时间到，恢复到目标状态
            state_ = retry_target_state_;
        }
    }

    void start_rollback() {
        log_info("Starting compaction rollback for src file %d", src_file_->file_id);

        // 1. 撤销已更新的索引 (先恢复索引，再删除文件)
        for (auto& update : committed_updates_) {
            revert_index_update(update);
        }
        committed_updates_.clear();

        // 2. 删除已写入的目标文件 (避免留下垃圾文件)
        if (dest_file_ && dest_offset_ > 0) {
            delete_garbage_file();
        } else {
            // 没有目标文件需要删除，直接恢复源文件状态
            state_ = State::ROLLBACK_MARK_SEALED;
        }
    }

    // 删除垃圾文件 (Compaction 失败时产生的部分写入文件)
    void delete_garbage_file() {
        dest_file_->state = FileState::DELETED;
        io_pending_ = true;

        log_info("Deleting garbage file %d (partial write: %lu bytes)",
                dest_file_->file_id, dest_offset_);

        // 异步删除 blob
        spdk_blob_close(dest_file_->blob, on_blob_closed_for_delete, this);
    }

    static void on_blob_closed_for_delete(void* arg, int status) {
        auto* self = static_cast<CompactionTask*>(arg);

        if (status != 0) {
            log_error("Failed to close blob for deletion: %d", status);
            // 继续尝试删除
        }

        // 删除 blob
        spdk_bs_delete_blob(self->scheduler_->engine_->blob_store(),
                           self->dest_file_->blob_id,
                           on_blob_deleted, self);
    }

    static void on_blob_deleted(void* arg, int status) {
        auto* self = static_cast<CompactionTask*>(arg);
        self->io_pending_ = false;

        if (status == 0) {
            log_info("Garbage file %d deleted successfully", self->dest_file_->file_id);

            // 从文件映射中移除
            self->scheduler_->engine_->remove_file_mapping(self->dest_file_->file_id);
        } else {
            log_error("Failed to delete garbage blob: %d (will be cleaned up on restart)",
                     status);
            // 删除失败时，文件仍标记为 DELETED，重启时会被清理
        }

        self->dest_file_ = nullptr;

        // 继续恢复源文件状态
        self->state_ = State::ROLLBACK_MARK_SEALED;
        self->step();  // 继续执行状态机
    }

    void revert_index_update(const IndexUpdate& update) {
        auto* existing = scheduler_->engine_->mem_index()->find(update.key);

        // 仅当索引当前指向新位置时才恢复
        if (existing &&
            existing->file_id == update.new_file_id &&
            existing->offset_index == update.new_offset_index) {

            MemIndexEntry old_entry = *existing;
            old_entry.file_id = update.old_file_id;
            old_entry.offset_index = update.old_offset_index;
            old_entry.page_count = update.page_count;

            scheduler_->engine_->mem_index()->upsert(update.key, old_entry);
        }
    }

    void rollback_mark_sealed() {
        src_file_->state = FileState::SEALED;  // 改回SEALED状态
        io_pending_ = true;

        update_file_header_async(src_file_, [this](int status) {
            io_pending_ = false;

            // 释放buffer
            if (read_buffer_) {
                scheduler_->engine_->pools().small_pool.free(read_buffer_);
                read_buffer_ = nullptr;
            }
            if (write_buffer_) {
                scheduler_->engine_->pools().small_pool.free(write_buffer_);
                write_buffer_ = nullptr;
            }

            if (status == 0) {
                log_info("Compaction rollback completed for file %d",
                        src_file_->file_id);
            } else {
                log_error("Failed to rollback file state, error: %d", status);
            }

            state_ = State::FAILED;
        });
    }

    static uint64_t get_current_time_us() {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
    }

    static constexpr size_t CHUNK_SIZE = 1 * 1024 * 1024;

    struct IndexUpdate {
        uint64_t key;
        uint16_t old_file_id;
        uint32_t old_offset_index;
        uint16_t new_file_id;
        uint32_t new_offset_index;
        uint16_t page_count;
    };

    FileInfo* src_file_;
    FileInfo* dest_file_;
    CompactionScheduler* scheduler_;
    State state_;

    uint64_t current_offset_;
    uint64_t dest_offset_;
    size_t process_offset_;
    size_t bytes_read_;

    void* read_buffer_;
    void* write_buffer_;
    size_t write_buffer_used_;

    bool io_pending_;
    std::vector<IndexUpdate> pending_updates_;
    std::vector<IndexUpdate> committed_updates_;  // 已提交的更新 (用于回滚)
    struct spdk_io_channel* io_channel_;

    // 重试相关字段
    int retry_count_ = 0;
    int last_error_ = 0;
    State retry_target_state_;
    uint64_t retry_timestamp_ = 0;
};
```

---

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
