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

