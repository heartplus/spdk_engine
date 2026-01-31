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

