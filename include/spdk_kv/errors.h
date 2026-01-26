// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024 SPDK KV Engine Authors

#pragma once

#include <cstdint>
#include <cerrno>

namespace spdk_kv {

// Error codes
enum class KvError : int32_t {
    SUCCESS = 0,
    KEY_NOT_FOUND = -1,
    VALUE_TOO_LARGE = -2,
    NO_SPACE = -3,
    IO_ERROR = -4,
    CORRUPTION = -5,
    INVALID_ARGUMENT = -6,
    ENGINE_NOT_READY = -7,
    INTERNAL_ERROR = -8,
    BACKPRESSURE = -9,
    ALREADY_EXISTS = -10,
    NOT_SUPPORTED = -11,
    TIMEOUT = -12,
    BUSY = -13,
};

// Convert error code to string
inline const char* error_to_string(KvError err) {
    switch (err) {
    case KvError::SUCCESS:
        return "Success";
    case KvError::KEY_NOT_FOUND:
        return "Key not found";
    case KvError::VALUE_TOO_LARGE:
        return "Value too large";
    case KvError::NO_SPACE:
        return "No space available";
    case KvError::IO_ERROR:
        return "IO error";
    case KvError::CORRUPTION:
        return "Data corruption";
    case KvError::INVALID_ARGUMENT:
        return "Invalid argument";
    case KvError::ENGINE_NOT_READY:
        return "Engine not ready";
    case KvError::INTERNAL_ERROR:
        return "Internal error";
    case KvError::BACKPRESSURE:
        return "Backpressure active";
    case KvError::ALREADY_EXISTS:
        return "Already exists";
    case KvError::NOT_SUPPORTED:
        return "Not supported";
    case KvError::TIMEOUT:
        return "Timeout";
    case KvError::BUSY:
        return "Resource busy";
    default:
        return "Unknown error";
    }
}

// Convert errno to KvError
inline KvError errno_to_kverror(int err) {
    switch (err) {
    case 0:
        return KvError::SUCCESS;
    case ENOENT:
        return KvError::KEY_NOT_FOUND;
    case EFBIG:
    case EOVERFLOW:
        return KvError::VALUE_TOO_LARGE;
    case ENOSPC:
        return KvError::NO_SPACE;
    case EIO:
        return KvError::IO_ERROR;
    case EINVAL:
        return KvError::INVALID_ARGUMENT;
    case EAGAIN:
        return KvError::BACKPRESSURE;
    case EEXIST:
        return KvError::ALREADY_EXISTS;
    case ENOTSUP:
        return KvError::NOT_SUPPORTED;
    case ETIMEDOUT:
        return KvError::TIMEOUT;
    case EBUSY:
        return KvError::BUSY;
    default:
        return KvError::INTERNAL_ERROR;
    }
}

}  // namespace spdk_kv
