// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#include "io_submitter.h"
#include "engine.h"

#ifdef WITH_SPDK
#include <spdk/blob.h>
#include <spdk/log.h>
#endif

namespace spdk_kv {

IoSubmitter::IoSubmitter(Engine* engine)
    : engine_(engine)
    , io_channel_(nullptr) {
}

IoSubmitter::~IoSubmitter() {
    // Clean up pending contexts
    while (!pending_writes_.empty()) {
        delete pending_writes_.front();
        pending_writes_.pop();
    }
    while (!pending_reads_.empty()) {
        delete pending_reads_.front();
        pending_reads_.pop();
    }
    for (auto* ctx : inflight_writes_) {
        delete ctx;
    }
    for (auto* ctx : inflight_reads_) {
        delete ctx;
    }
}

#ifdef WITH_SPDK
bool IoSubmitter::init(struct spdk_io_channel* channel) {
    io_channel_ = channel;
    return io_channel_ != nullptr;
}
#else
bool IoSubmitter::init(void* channel) {
    io_channel_ = channel;
    return true;
}
#endif

void IoSubmitter::poll() {
    // Submit pending writes in batches
    submit_write_batch();

    // Submit pending reads in batches
    submit_read_batch();
}

void IoSubmitter::submit_write(AppendBuffer* buffer, FileInfo* file, uint64_t offset,
                               std::vector<PendingWrite>&& pending_writes) {
    auto* ctx = new WriteContext{
        engine_,
        buffer,
        file,
        offset,
        buffer->used(),
        std::move(pending_writes),
        nullptr,
        nullptr
    };
    pending_writes_.push(ctx);
}

void IoSubmitter::submit_read(uint64_t key, void* buffer, size_t buffer_size,
                              FileInfo* file, uint64_t offset, size_t size,
                              spdk_kv_cb cb, void* cb_arg) {
    auto* ctx = new ReadContext{
        engine_,
        key,
        buffer,
        buffer_size,
        0,
        file,
        offset,
        size,
        cb,
        cb_arg
    };
    pending_reads_.push(ctx);
}

#ifdef WITH_SPDK

// SPDK write completion callback
static void on_write_complete(void* arg, int bserrno) {
    auto* ctx = static_cast<WriteContext*>(arg);

    if (bserrno == 0) {
        // Update indices for successful writes
        for (auto& pw : ctx->pending_writes) {
            ctx->engine->on_write_complete(pw, 0);
        }
    } else {
        // Handle write errors
        for (auto& pw : ctx->pending_writes) {
            ctx->engine->on_write_complete(pw, bserrno);
        }
    }

    // Return buffer to manager
    ctx->engine->buffer_manager()->return_buffer(ctx->buffer);

    delete ctx;
}

// SPDK read completion callback
static void on_read_complete(void* arg, int bserrno) {
    auto* ctx = static_cast<ReadContext*>(arg);

    if (ctx->callback) {
        ctx->callback(ctx->cb_arg, bserrno);
    }

    delete ctx;
}

void IoSubmitter::submit_write_batch() {
    size_t count = 0;
    while (count < BATCH_SIZE && !pending_writes_.empty()) {
        auto* ctx = pending_writes_.front();
        pending_writes_.pop();

        // Check if buffer can be submitted
        if (!ctx->buffer->can_submit()) {
            // RDMA writes not complete, put back
            pending_writes_.push(ctx);
            continue;
        }

        // Submit write to SPDK
        spdk_blob_io_write(ctx->file->blob, io_channel_,
                          ctx->buffer->data(),
                          ctx->offset / ALIGNMENT,
                          ctx->size / ALIGNMENT,
                          on_write_complete, ctx);
        count++;
    }
}

void IoSubmitter::submit_read_batch() {
    size_t count = 0;
    while (count < BATCH_SIZE && !pending_reads_.empty()) {
        auto* ctx = pending_reads_.front();
        pending_reads_.pop();

        // Submit read to SPDK
        spdk_blob_io_read(ctx->file->blob, io_channel_,
                         ctx->buffer,
                         ctx->offset / ALIGNMENT,
                         ctx->size / ALIGNMENT,
                         on_read_complete, ctx);
        count++;
    }
}

void IoSubmitter::process_completions() {
    // SPDK completions are processed via polling
    // This is handled by the main engine poll loop
}

#else  // Non-SPDK implementation (simulation)

void IoSubmitter::submit_write_batch() {
    while (!pending_writes_.empty()) {
        auto* ctx = pending_writes_.front();
        pending_writes_.pop();

        // Simulate successful write
        for (auto& pw : ctx->pending_writes) {
            ctx->engine->on_write_complete(pw, 0);
        }

        ctx->engine->buffer_manager()->return_buffer(ctx->buffer);
        delete ctx;
    }
}

void IoSubmitter::submit_read_batch() {
    while (!pending_reads_.empty()) {
        auto* ctx = pending_reads_.front();
        pending_reads_.pop();

        // Simulate read (in real implementation, would copy from storage)
        if (ctx->callback) {
            ctx->callback(ctx->cb_arg, 0);
        }

        delete ctx;
    }
}

void IoSubmitter::process_completions() {
    // No-op for simulation
}

#endif

}  // namespace spdk_kv
