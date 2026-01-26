// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#include "spdk_kv/io_submitter.h"

#include <cstring>

#include "spdk_kv/spdk_env.h"

namespace spdk_kv {

IoSubmitter::IoSubmitter()
        :
#ifdef WITH_SPDK
          ctrlr_(nullptr),
          ns_(nullptr),
          qpair_(nullptr),
          blobstore_(nullptr),
          io_channel_(nullptr),
#endif
          simulation_mode_(true) {
}

IoSubmitter::~IoSubmitter() {
#ifdef WITH_SPDK
    if (qpair_) {
        spdk_nvme_ctrlr_free_io_qpair(qpair_);
    }
    if (io_channel_) {
        spdk_bs_free_io_channel(io_channel_);
    }
#endif
}

#ifdef WITH_SPDK
bool IoSubmitter::Initialize(spdk_nvme_ctrlr* ctrlr, spdk_nvme_ns* ns,
                             spdk_blob_store* blobstore) {
    ctrlr_ = ctrlr;
    ns_ = ns;
    blobstore_ = blobstore;

    // Create IO queue pair
    struct spdk_nvme_io_qpair_opts opts;
    spdk_nvme_ctrlr_get_default_io_qpair_opts(ctrlr, &opts, sizeof(opts));
    opts.io_queue_size = 256;  // Can be tuned

    qpair_ = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, &opts, sizeof(opts));
    if (!qpair_) {
        return false;
    }

    // Get IO channel
    io_channel_ = spdk_bs_alloc_io_channel(blobstore);
    if (!io_channel_) {
        spdk_nvme_ctrlr_free_io_qpair(qpair_);
        qpair_ = nullptr;
        return false;
    }

    simulation_mode_ = false;
    return true;
}
#endif

bool IoSubmitter::InitializeSimulation() {
    simulation_mode_ = true;
    return true;
}

void IoSubmitter::SubmitWrite(AppendBuffer* buffer, uint64_t file_offset,
                              const std::vector<WriteContext>& contexts,
                              IoCompletionCallback callback) {
    PendingWrite pw;
    pw.buffer = buffer;
    pw.offset = file_offset;
    pw.contexts = contexts;
    pw.callback = std::move(callback);
    pending_writes_.push(std::move(pw));

    if (simulation_mode_) {
        // In simulation mode, complete immediately
        // The actual data was already written to the buffer in Engine::Put
        completed_callbacks_.push({pw.callback, 0});
    }
}

void IoSubmitter::SubmitRead(uint16_t file_id, uint64_t offset, uint32_t pages, void* buffer,
                             IoCompletionCallback callback) {
    auto* ctx = new ReadContext;
    ctx->file_id = file_id;
    ctx->offset = offset;
    ctx->pages = pages;
    ctx->buffer = buffer;
    ctx->callback = std::move(callback);
    pending_reads_.push(ctx);

    if (simulation_mode_) {
        // In simulation mode, complete immediately
        // Data should already be in the buffer (managed by Engine)
        completed_callbacks_.push({ctx->callback, 0});
    }
}

#ifdef WITH_SPDK
void IoSubmitter::SubmitBlobRead(spdk_blob* blob, uint64_t offset, uint32_t pages, void* buffer,
                                 IoCompletionCallback callback) {
    if (!blob || !io_channel_ || !blobstore_) {
        if (callback) {
            callback(-1);
        }
        return;
    }

    // Create a completion context
    struct ReadCompletionCtx {
        IoCompletionCallback callback;
    };
    auto* ctx = new ReadCompletionCtx{std::move(callback)};

    // Calculate offset and length in io_unit_size
    uint64_t io_unit_size = spdk_bs_get_io_unit_size(blobstore_);
    uint64_t offset_units = offset / io_unit_size;
    uint64_t length_units = (pages * kPageSize) / io_unit_size;

    spdk_blob_io_read(
            blob, io_channel_, buffer, offset_units, length_units,
            [](void* arg, int bserrno) {
                auto* ctx = static_cast<ReadCompletionCtx*>(arg);
                if (ctx->callback) {
                    ctx->callback(bserrno);
                }
                delete ctx;
            },
            ctx);
}
#endif

size_t IoSubmitter::ProcessCompletions(size_t max_completions) {
    size_t count = 0;

#ifdef WITH_SPDK
    if (!simulation_mode_ && qpair_) {
        // Process NVMe completions
        count = spdk_nvme_qpair_process_completions(qpair_, max_completions);
    }
#endif

    // Process simulation callbacks
    while (!completed_callbacks_.empty() && count < max_completions) {
        auto& cb = completed_callbacks_.front();
        if (cb.first) {
            cb.first(cb.second);
        }
        completed_callbacks_.pop();
        count++;
    }

    // Cleanup completed reads
    while (!pending_reads_.empty()) {
        auto* ctx = pending_reads_.front();
        pending_reads_.pop();
        delete ctx;
    }

    return count;
}

void IoSubmitter::FlushPendingWrites() {
    if (simulation_mode_) {
        // In simulation mode, complete all pending writes immediately
        while (!pending_writes_.empty()) {
            auto& pw = pending_writes_.front();
            if (pw.callback) {
                pw.callback(0);
            }
            pending_writes_.pop();
        }
    }
#ifdef WITH_SPDK
    else {
        // Submit batch to NVMe
        SubmitBatch();
    }
#endif
}

void IoSubmitter::SubmitBatch() {
#ifdef WITH_SPDK
    if (pending_writes_.empty() || !io_channel_) {
        return;
    }

    // Submit writes in batch using blob IO
    while (!pending_writes_.empty()) {
        auto pw = std::move(pending_writes_.front());
        pending_writes_.pop();

        if (!pw.contexts.empty() && pw.contexts[0].blob && pw.buffer) {
            // Create a completion context
            struct WriteCompletionCtx {
                IoCompletionCallback callback;
            };
            auto* ctx = new WriteCompletionCtx{std::move(pw.callback)};

            // Submit blob IO write
            // offset and length are in units of io_unit_size (usually 512 bytes)
            uint64_t io_unit_size = spdk_bs_get_io_unit_size(blobstore_);
            uint64_t offset_units = pw.offset / io_unit_size;
            uint64_t length_units = pw.buffer->Used() / io_unit_size;

            spdk_blob_io_write(
                    pw.contexts[0].blob, io_channel_, pw.buffer->Data(), offset_units,
                    length_units,
                    [](void* arg, int bserrno) {
                        auto* ctx = static_cast<WriteCompletionCtx*>(arg);
                        if (ctx->callback) {
                            ctx->callback(bserrno);
                        }
                        delete ctx;
                    },
                    ctx);
        } else {
            // No blob available, complete with error
            if (pw.callback) {
                pw.callback(-1);
            }
        }
    }

    // Process any immediate completions
    spdk_nvme_qpair_process_completions(qpair_, 0);
#endif
}

void IoSubmitter::OnWriteComplete(int status, const std::vector<WriteContext>& contexts) {
    (void)status;
    (void)contexts;
    // Handle write completion
    // Update indices, call user callbacks, etc.
}

void IoSubmitter::OnReadComplete(int status, ReadContext* ctx) {
    if (ctx->callback) {
        ctx->callback(status);
    }
    delete ctx;
}

}  // namespace spdk_kv
