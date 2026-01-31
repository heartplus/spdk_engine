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

