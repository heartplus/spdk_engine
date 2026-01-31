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


