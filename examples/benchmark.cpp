// SPDX-License-Identifier: Apache-2.0
// Benchmark for SPDK KV Engine

// Include internal headers for direct Engine access
#include "../src/engine.h"
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <chrono>
#include <atomic>
#include <vector>
#include <random>
#include <thread>

using namespace spdk_kv;

// Benchmark configuration
struct BenchConfig {
    uint64_t num_keys = 100000;
    size_t value_size = 4096;
    uint32_t num_threads = 1;  // Simulated, actual work is single-threaded
    bool random_keys = false;
    double read_ratio = 0.5;  // 50% reads, 50% writes
};

// Statistics
struct BenchStats {
    std::atomic<uint64_t> total_ops{0};
    std::atomic<uint64_t> total_puts{0};
    std::atomic<uint64_t> total_gets{0};
    std::atomic<uint64_t> failed_ops{0};
    std::chrono::high_resolution_clock::time_point start_time;
    std::chrono::high_resolution_clock::time_point end_time;
};

static BenchStats stats;
static std::atomic<bool> benchmark_running{false};

static void on_op_complete(void* arg, int status) {
    (void)arg;
    if (status == 0) {
        stats.total_ops++;
    } else {
        stats.failed_ops++;
    }
}

void print_usage(const char* prog) {
    printf("Usage: %s [options] [device]\n", prog);
    printf("Options:\n");
    printf("  -n <num>     Number of keys (default: 100000)\n");
    printf("  -s <size>    Value size in bytes (default: 4096)\n");
    printf("  -r           Use random keys (default: sequential)\n");
    printf("  -R <ratio>   Read ratio 0.0-1.0 (default: 0.5)\n");
    printf("  -h           Show this help\n");
}

void run_sequential_benchmark(Engine* engine, const BenchConfig& config) {
    printf("Running sequential benchmark...\n");
    printf("  Keys: %lu\n", config.num_keys);
    printf("  Value size: %zu bytes\n", config.value_size);
    printf("\n");

    // Allocate buffers
    std::vector<void*> values(config.num_keys);
    for (uint64_t i = 0; i < config.num_keys; i++) {
        values[i] = engine->alloc_buffer(config.value_size);
        if (!values[i]) {
            printf("Failed to allocate buffer %lu\n", i);
            return;
        }
        memset(values[i], 'X', config.value_size);
        snprintf(static_cast<char*>(values[i]), config.value_size,
                 "benchmark-value-%lu", i);
    }

    // Phase 1: Sequential writes
    printf("Phase 1: Sequential writes...\n");
    stats.total_ops = 0;
    stats.failed_ops = 0;
    stats.start_time = std::chrono::high_resolution_clock::now();

    for (uint64_t i = 0; i < config.num_keys; i++) {
        int rc = engine->put(i, values[i], config.value_size, on_op_complete, nullptr);
        if (rc != 0) {
            stats.failed_ops++;
        }

        // Poll every 100 operations
        if (i % 100 == 0) {
            engine->poll();
        }
    }

    // Wait for all completions
    while (stats.total_ops.load() + stats.failed_ops.load() < config.num_keys) {
        engine->poll();
    }

    stats.end_time = std::chrono::high_resolution_clock::now();

    auto write_duration = std::chrono::duration_cast<std::chrono::microseconds>(
        stats.end_time - stats.start_time).count();
    double write_iops = (stats.total_ops.load() * 1000000.0) / write_duration;
    double write_throughput = (stats.total_ops.load() * config.value_size * 1000000.0) /
                              (write_duration * 1024 * 1024);

    printf("  Completed: %lu ops in %.3f seconds\n",
           stats.total_ops.load(), write_duration / 1000000.0);
    printf("  Write IOPS: %.0f\n", write_iops);
    printf("  Throughput: %.2f MB/s\n", write_throughput);
    printf("  Failed: %lu\n\n", stats.failed_ops.load());

    // Phase 2: Sequential reads
    printf("Phase 2: Sequential reads...\n");
    stats.total_ops = 0;
    stats.failed_ops = 0;

    void* read_buf = engine->alloc_buffer(config.value_size);
    if (!read_buf) {
        printf("Failed to allocate read buffer\n");
        return;
    }

    stats.start_time = std::chrono::high_resolution_clock::now();

    for (uint64_t i = 0; i < config.num_keys; i++) {
        int rc = engine->get(i, read_buf, config.value_size, on_op_complete, nullptr);
        if (rc != 0) {
            stats.failed_ops++;
        }

        if (i % 100 == 0) {
            engine->poll();
        }
    }

    while (stats.total_ops.load() + stats.failed_ops.load() < config.num_keys) {
        engine->poll();
    }

    stats.end_time = std::chrono::high_resolution_clock::now();

    auto read_duration = std::chrono::duration_cast<std::chrono::microseconds>(
        stats.end_time - stats.start_time).count();
    double read_iops = (stats.total_ops.load() * 1000000.0) / read_duration;
    double read_throughput = (stats.total_ops.load() * config.value_size * 1000000.0) /
                             (read_duration * 1024 * 1024);

    printf("  Completed: %lu ops in %.3f seconds\n",
           stats.total_ops.load(), read_duration / 1000000.0);
    printf("  Read IOPS: %.0f\n", read_iops);
    printf("  Throughput: %.2f MB/s\n", read_throughput);
    printf("  Failed: %lu\n\n", stats.failed_ops.load());

    // Phase 3: Mixed workload
    printf("Phase 3: Mixed workload (%.0f%% reads)...\n", config.read_ratio * 100);
    stats.total_ops = 0;
    stats.failed_ops = 0;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dist(0.0, 1.0);
    std::uniform_int_distribution<uint64_t> key_dist(0, config.num_keys - 1);

    stats.start_time = std::chrono::high_resolution_clock::now();

    for (uint64_t i = 0; i < config.num_keys; i++) {
        uint64_t key = config.random_keys ? key_dist(gen) : i % config.num_keys;
        int rc;

        if (dist(gen) < config.read_ratio) {
            rc = engine->get(key, read_buf, config.value_size, on_op_complete, nullptr);
        } else {
            rc = engine->put(key, values[key % values.size()], config.value_size,
                            on_op_complete, nullptr);
        }

        if (rc != 0) {
            stats.failed_ops++;
        }

        if (i % 100 == 0) {
            engine->poll();
        }
    }

    while (stats.total_ops.load() + stats.failed_ops.load() < config.num_keys) {
        engine->poll();
    }

    stats.end_time = std::chrono::high_resolution_clock::now();

    auto mixed_duration = std::chrono::duration_cast<std::chrono::microseconds>(
        stats.end_time - stats.start_time).count();
    double mixed_iops = (stats.total_ops.load() * 1000000.0) / mixed_duration;

    printf("  Completed: %lu ops in %.3f seconds\n",
           stats.total_ops.load(), mixed_duration / 1000000.0);
    printf("  Mixed IOPS: %.0f\n", mixed_iops);
    printf("  Failed: %lu\n\n", stats.failed_ops.load());

    // Cleanup
    engine->free_buffer(read_buf);
    for (auto* v : values) {
        engine->free_buffer(v);
    }

    // Print summary
    printf("=== Benchmark Summary ===\n");
    printf("Write IOPS: %.0f\n", write_iops);
    printf("Read IOPS: %.0f\n", read_iops);
    printf("Mixed IOPS (%.0f%% reads): %.0f\n", config.read_ratio * 100, mixed_iops);
    printf("Write throughput: %.2f MB/s\n", write_throughput);
    printf("Read throughput: %.2f MB/s\n", read_throughput);
}

int main(int argc, char** argv) {
    printf("SPDK KV Engine Benchmark\n");
    printf("========================\n\n");

    BenchConfig config;
    const char* dev_name = "simulation";

    // Parse arguments
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-n") == 0 && i + 1 < argc) {
            config.num_keys = strtoull(argv[++i], nullptr, 10);
        } else if (strcmp(argv[i], "-s") == 0 && i + 1 < argc) {
            config.value_size = strtoull(argv[++i], nullptr, 10);
        } else if (strcmp(argv[i], "-r") == 0) {
            config.random_keys = true;
        } else if (strcmp(argv[i], "-R") == 0 && i + 1 < argc) {
            config.read_ratio = atof(argv[++i]);
        } else if (strcmp(argv[i], "-h") == 0) {
            print_usage(argv[0]);
            return 0;
        } else if (argv[i][0] != '-') {
            dev_name = argv[i];
        }
    }

    // Create engine
    CreateOpts create_opts;
    create_opts.config.max_entries = config.num_keys * 2;
    create_opts.config.data_file_size = 4ULL * 1024 * 1024 * 1024;  // 4GB

    auto* engine = new Engine();

    int rc = engine->create(dev_name, &create_opts,
        [](void* arg, int status) {
            printf("Engine creation completed with status: %d\n", status);
        }, nullptr);

    if (rc != 0) {
        printf("Failed to create engine: %d\n", rc);
        delete engine;
        return 1;
    }

    // Wait for engine to be ready
    while (!engine->is_ready()) {
        engine->poll();
    }

    printf("Engine ready on device: %s\n\n", dev_name);

    // Run benchmark
    run_sequential_benchmark(engine, config);

    // Close engine
    bool close_complete = false;
    engine->close([](void* arg, int status) {
        *static_cast<bool*>(arg) = true;
    }, &close_complete);

    while (!close_complete) {
        // Wait
    }

    printf("\nBenchmark completed.\n");

    return 0;
}
