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

