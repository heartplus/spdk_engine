// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

// Basic example demonstrating SPDK KV Engine usage

#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "spdk_kv/spdk_kv.h"

using namespace spdk_kv;

int main() {
    std::cout << "=== SPDK KV Engine Basic Example ===" << std::endl;
    std::cout << std::endl;

    // create engine
    Engine engine;
    CreateOpts create_opts;
    create_opts.config.max_entries = 1000000;              // 1M entries for testing
    create_opts.config.data_file_size = 64 * 1024 * 1024;  // 64MB for testing

    std::cout << "Creating engine..." << std::endl;
    KvError err = engine.create("/tmp/spdk_kv_test", create_opts);
    if (err != KvError::kSuccess) {
        std::cerr << "Failed to create engine: " << static_cast<int>(err) << std::endl;
        return 1;
    }
    std::cout << "Engine created successfully!" << std::endl;
    std::cout << std::endl;

    // put some key-value pairs
    std::cout << "--- put Operations ---" << std::endl;
    const int kNumEntries = 100;
    for (int i = 0; i < kNumEntries; i++) {
        uint64_t key = i + 1;
        std::string value = "value_" + std::to_string(i);

        err = engine.put(key, value.data(), static_cast<uint32_t>(value.size()));
        if (err != KvError::kSuccess) {
            std::cerr << "Failed to put key " << key << ": " << static_cast<int>(err) << std::endl;
            return 1;
        }
    }
    std::cout << "Successfully put " << kNumEntries << " key-value pairs" << std::endl;
    std::cout << "Entry count: " << engine.get_entry_count() << std::endl;
    std::cout << "Total data bytes: " << engine.get_total_data_bytes() << std::endl;
    std::cout << "Index load factor: " << engine.get_index_load_factor() << std::endl;
    std::cout << std::endl;

    // get some values
    std::cout << "--- get Operations ---" << std::endl;
    char buffer[256];
    for (int i = 0; i < 5; i++) {
        uint64_t key = i + 1;
        uint32_t actual_len = 0;

        err = engine.get(key, buffer, sizeof(buffer), &actual_len);
        if (err != KvError::kSuccess) {
            std::cerr << "Failed to get key " << key << ": " << static_cast<int>(err) << std::endl;
            return 1;
        }

        buffer[actual_len] = '\0';
        std::cout << "Key " << key << ": " << buffer << " (len=" << actual_len << ")" << std::endl;
    }
    std::cout << std::endl;

    // Update a value
    std::cout << "--- Update Operation ---" << std::endl;
    {
        uint64_t key = 1;
        std::string new_value = "updated_value_1";

        err = engine.put(key, new_value.data(), static_cast<uint32_t>(new_value.size()));
        if (err != KvError::kSuccess) {
            std::cerr << "Failed to update key " << key << ": " << static_cast<int>(err)
                      << std::endl;
            return 1;
        }

        uint32_t actual_len = 0;
        err = engine.get(key, buffer, sizeof(buffer), &actual_len);
        if (err != KvError::kSuccess) {
            std::cerr << "Failed to get updated key " << key << ": " << static_cast<int>(err)
                      << std::endl;
            return 1;
        }

        buffer[actual_len] = '\0';
        std::cout << "Updated key " << key << ": " << buffer << std::endl;
    }
    std::cout << std::endl;

    // del a key
    std::cout << "--- del Operation ---" << std::endl;
    {
        uint64_t key = 50;

        // First verify it exists
        uint32_t actual_len = 0;
        err = engine.get(key, buffer, sizeof(buffer), &actual_len);
        if (err == KvError::kSuccess) {
            buffer[actual_len] = '\0';
            std::cout << "Before delete - Key " << key << ": " << buffer << std::endl;
        }

        // del it
        err = engine.del(key);
        if (err != KvError::kSuccess) {
            std::cerr << "Failed to delete key " << key << ": " << static_cast<int>(err)
                      << std::endl;
            return 1;
        }

        // Verify it's gone
        err = engine.get(key, buffer, sizeof(buffer), &actual_len);
        if (err == KvError::kKeyNotFound) {
            std::cout << "After delete - Key " << key << " not found (as expected)" << std::endl;
        } else {
            std::cerr << "Key " << key << " should not exist after delete!" << std::endl;
            return 1;
        }
    }
    std::cout << std::endl;

    // get non-existent key
    std::cout << "--- Non-existent Key ---" << std::endl;
    {
        uint64_t key = 99999;
        uint32_t actual_len = 0;

        err = engine.get(key, buffer, sizeof(buffer), &actual_len);
        if (err == KvError::kKeyNotFound) {
            std::cout << "Key " << key << " not found (expected)" << std::endl;
        } else {
            std::cerr << "Unexpected result for non-existent key" << std::endl;
            return 1;
        }
    }
    std::cout << std::endl;

    // Final statistics
    std::cout << "--- Final Statistics ---" << std::endl;
    std::cout << "Entry count: " << engine.get_entry_count() << std::endl;
    std::cout << "Total data bytes: " << engine.get_total_data_bytes() << std::endl;
    std::cout << "Index load factor: " << engine.get_index_load_factor() << std::endl;
    std::cout << std::endl;

    // close engine
    std::cout << "Closing engine..." << std::endl;
    err = engine.close();
    if (err != KvError::kSuccess) {
        std::cerr << "Failed to close engine: " << static_cast<int>(err) << std::endl;
        return 1;
    }
    std::cout << "Engine closed successfully!" << std::endl;

    std::cout << std::endl;
    std::cout << "=== Example completed successfully! ===" << std::endl;
    return 0;
}
