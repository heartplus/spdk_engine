// SPDX-License-Identifier: Apache-2.0
// Copyright 2024 SPDK KV Engine Authors

// Benchmark example for SPDK KV Engine

#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "spdk_kv/spdk_kv.h"

using namespace spdk_kv;

// Timer utility
class Timer {
public:
    void Start() { start_ = std::chrono::high_resolution_clock::now(); }

    double ElapsedSeconds() const {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double>(end - start_).count();
    }

    double ElapsedMilliseconds() const { return ElapsedSeconds() * 1000.0; }

private:
    std::chrono::high_resolution_clock::time_point start_;
};

// Format number with commas
std::string FormatNumber(uint64_t n) {
    std::string str = std::to_string(n);
    int pos = static_cast<int>(str.length()) - 3;
    while (pos > 0) {
        str.insert(pos, ",");
        pos -= 3;
    }
    return str;
}

// Run benchmark
void RunBenchmark(Engine& engine, int num_ops, int value_size) {
    Timer timer;
    std::vector<char> value(value_size);
    std::vector<char> read_buffer(value_size + 1024);
    std::random_device rd;
    std::mt19937_64 rng(rd());

    // Fill value with random data
    for (int i = 0; i < value_size; i++) {
        value[i] = static_cast<char>(rng() & 0xFF);
    }

    // Sequential Write Benchmark
    std::cout << "  Sequential Write..." << std::flush;
    timer.Start();
    for (int i = 0; i < num_ops; i++) {
        uint64_t key = i + 1;
        KvError err = engine.Put(key, value.data(), value_size);
        if (err != KvError::kSuccess) {
            std::cerr << "\nWrite failed at key " << key << std::endl;
            return;
        }
    }
    double write_time = timer.ElapsedSeconds();
    double write_ops = num_ops / write_time;
    double write_mbps = (static_cast<double>(num_ops) * value_size) / write_time / (1024 * 1024);
    std::cout << " " << std::fixed << std::setprecision(2) << write_ops << " ops/s, " << write_mbps
              << " MB/s" << std::endl;

    // Sequential Read Benchmark
    std::cout << "  Sequential Read..." << std::flush;
    timer.Start();
    for (int i = 0; i < num_ops; i++) {
        uint64_t key = i + 1;
        uint32_t actual_len = 0;
        KvError err = engine.Get(key, read_buffer.data(), read_buffer.size(), &actual_len);
        if (err != KvError::kSuccess) {
            std::cerr << "\nRead failed at key " << key << std::endl;
            return;
        }
    }
    double read_time = timer.ElapsedSeconds();
    double read_ops = num_ops / read_time;
    double read_mbps = (static_cast<double>(num_ops) * value_size) / read_time / (1024 * 1024);
    std::cout << " " << std::fixed << std::setprecision(2) << read_ops << " ops/s, " << read_mbps
              << " MB/s" << std::endl;

    // Random Read Benchmark
    std::cout << "  Random Read..." << std::flush;
    std::uniform_int_distribution<uint64_t> dist(1, num_ops);
    timer.Start();
    for (int i = 0; i < num_ops; i++) {
        uint64_t key = dist(rng);
        uint32_t actual_len = 0;
        KvError err = engine.Get(key, read_buffer.data(), read_buffer.size(), &actual_len);
        if (err != KvError::kSuccess) {
            std::cerr << "\nRandom read failed at key " << key << std::endl;
            return;
        }
    }
    double rand_read_time = timer.ElapsedSeconds();
    double rand_read_ops = num_ops / rand_read_time;
    std::cout << " " << std::fixed << std::setprecision(2) << rand_read_ops << " ops/s"
              << std::endl;

    // Update Benchmark
    std::cout << "  Random Update..." << std::flush;
    timer.Start();
    for (int i = 0; i < num_ops; i++) {
        uint64_t key = dist(rng);
        KvError err = engine.Put(key, value.data(), value_size);
        if (err != KvError::kSuccess) {
            std::cerr << "\nUpdate failed at key " << key << std::endl;
            return;
        }
    }
    double update_time = timer.ElapsedSeconds();
    double update_ops = num_ops / update_time;
    std::cout << " " << std::fixed << std::setprecision(2) << update_ops << " ops/s" << std::endl;
}

int main(int argc, char* argv[]) {
    std::cout << "=== SPDK KV Engine Benchmark ===" << std::endl;
    std::cout << std::endl;

    // Parse arguments
    int num_ops = 100000;            // Default 100K ops
    int value_size = 1024;           // Default 1KB value
    uint64_t max_entries = 1000000;  // 1M entries

    if (argc > 1) num_ops = std::atoi(argv[1]);
    if (argc > 2) value_size = std::atoi(argv[2]);

    std::cout << "Configuration:" << std::endl;
    std::cout << "  Operations: " << FormatNumber(num_ops) << std::endl;
    std::cout << "  Value size: " << value_size << " bytes" << std::endl;
    std::cout << "  Max entries: " << FormatNumber(max_entries) << std::endl;
    std::cout << std::endl;

    // Create engine
    Engine engine;
    CreateOpts create_opts;
    create_opts.config.max_entries = max_entries;
    create_opts.config.data_file_size = 512 * 1024 * 1024;  // 512MB

    std::cout << "Creating engine..." << std::endl;
    KvError err = engine.Create("/tmp/spdk_kv_bench", create_opts);
    if (err != KvError::kSuccess) {
        std::cerr << "Failed to create engine: " << static_cast<int>(err) << std::endl;
        return 1;
    }
    std::cout << "Engine ready!" << std::endl;
    std::cout << std::endl;

    // Run benchmarks
    std::cout << "Running benchmarks..." << std::endl;
    RunBenchmark(engine, num_ops, value_size);
    std::cout << std::endl;

    // Statistics
    std::cout << "Final Statistics:" << std::endl;
    std::cout << "  Entry count: " << FormatNumber(engine.GetEntryCount()) << std::endl;
    std::cout << "  Total data: " << FormatNumber(engine.GetTotalDataBytes()) << " bytes"
              << std::endl;
    std::cout << "  Load factor: " << std::fixed << std::setprecision(4)
              << engine.GetIndexLoadFactor() << std::endl;
    std::cout << std::endl;

    // Close
    engine.Close();
    std::cout << "Benchmark completed!" << std::endl;

    return 0;
}
