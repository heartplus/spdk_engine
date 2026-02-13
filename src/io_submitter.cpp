// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

#include "spdk_kv/io_submitter.h"

#include <cstring>

#include "spdk_kv/spdk_env.h"

namespace spdk_kv {

IoSubmitter::IoSubmitter()
        : ctrlr_(nullptr),
          ns_(nullptr),
          qpair_(nullptr),
          blobstore_(nullptr),
          io_channel_(nullptr) {}

IoSubmitter::~IoSubmitter() {
    if (qpair_) {
        spdk_nvme_ctrlr_free_io_qpair(qpair_);
    }
    if (io_channel_) {
        spdk_bs_free_io_channel(io_channel_);
    }
}

bool IoSubmitter::Initialize(spdk_nvme_ctrlr* ctrlr, spdk_nvme_ns* ns, spdk_blob_store* blobstore,
                             uint32_t io_queue_size) {
    ctrlr_ = ctrlr;
    ns_ = ns;
    blobstore_ = blobstore;

    // Create IO queue pair
    struct spdk_nvme_io_qpair_opts opts;
    spdk_nvme_ctrlr_get_default_io_qpair_opts(ctrlr, &opts, sizeof(opts));
    opts.io_queue_size = io_queue_size;

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
}

void IoSubmitter::SubmitBlobRead(spdk_blob* blob, uint64_t offset, uint32_t pages, void* buffer,
                                 IoCompletionCallback callback) {
    if (!blob || !io_channel_ || !blobstore_) {
        if (callback) {
            callback(-1);
        }
        return;
    }

    // Create a completion context from pool
    auto* ctx = read_completion_ctx_pool_.Alloc(std::move(callback), this);

    // Calculate offset and length in io_unit_size
    uint64_t io_unit_size = spdk_bs_get_io_unit_size(blobstore_);
    uint64_t offset_units = offset / io_unit_size;
    uint64_t length_units = (pages * kPageSize) / io_unit_size;

    spdk_blob_io_read(
            blob, io_channel_, buffer, offset_units, length_units,
            [](void* arg, int bserrno) {
                auto* ctx = static_cast<ReadCompletionCtx*>(arg);
                auto cb = std::move(ctx->callback);
                ctx->submitter->read_completion_ctx_pool_.Free(ctx);
                if (cb) {
                    cb(bserrno);
                }
            },
            ctx);
}

size_t IoSubmitter::ProcessCompletions(size_t max_completions) {
    size_t count = 0;

    if (qpair_) {
        // Process NVMe completions
        count = spdk_nvme_qpair_process_completions(qpair_, max_completions);
    }

    return count;
}

void IoSubmitter::FlushPendingWrites() {
    // Submit batch to NVMe
    SubmitBatch();
}

void IoSubmitter::SubmitBatch() {
    if (pending_writes_.empty() || !io_channel_) {
        return;
    }

    // Submit writes in batch using blob IO
    while (!pending_writes_.empty()) {
        auto pw = std::move(pending_writes_.front());
        pending_writes_.pop();

        if (!pw.contexts.empty() && pw.contexts[0].blob && pw.buffer) {
            // Create a completion context from pool
            auto* ctx = write_completion_ctx_pool_.Alloc(std::move(pw.callback), this);

            // Submit blob IO write
            // offset and length are in units of io_unit_size (usually 512 bytes)
            uint64_t io_unit_size = spdk_bs_get_io_unit_size(blobstore_);
            uint64_t offset_units = pw.offset / io_unit_size;
            uint64_t length_units = pw.buffer->Used() / io_unit_size;

            spdk_blob_io_write(
                    pw.contexts[0].blob, io_channel_, pw.buffer->Data(), offset_units, length_units,
                    [](void* arg, int bserrno) {
                        auto* ctx = static_cast<WriteCompletionCtx*>(arg);
                        auto cb = std::move(ctx->callback);
                        ctx->submitter->write_completion_ctx_pool_.Free(ctx);
                        if (cb) {
                            cb(bserrno);
                        }
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
