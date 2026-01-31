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
