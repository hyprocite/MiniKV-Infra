#include "../include/minikv/single_lock_kv_store.hpp"
#include "../include/minikv/sharded_kv_store.hpp"
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iomanip>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

// 一组实验的配置。
struct BenchmarkConfig
{
    std::size_t thread_count = 1;
    std::size_t key_count = 10000;
    std::size_t operations_per_thread = 200000;
    std::size_t value_bytes = 64;

    unsigned int read_percent = 100;
    std::uint64_t seed = 42;
};

// 一次操作只保存键的编号，不重复保存字符串。
struct Operation
{
    std::size_t key_index;
    bool is_read;
};

// 提前准备负载，两个存储实现复用同一份操作序列。
struct Workload
{
    std::vector<std::string> keys;
    std::vector<std::vector<Operation>> thread_operations;
};

// 每个线程独立统计，循环中不争用全局计数器。
struct WorkerStats
{
    std::uint64_t reads = 0;
    std::uint64_t writes = 0;
    std::uint64_t errors = 0;
    std::uint64_t checksum = 0;
};

struct BenchmarkResult
{
    WorkerStats totals;
    double elapsed_seconds = 0.0;
};

Workload make_workload(const BenchmarkConfig& config)
{
    if (config.thread_count == 0 ||
        config.key_count == 0 ||
        config.operations_per_thread == 0 ||
        config.value_bytes == 0 ||
        config.read_percent > 100)
    {
        throw std::invalid_argument("invalid benchmark configuration");
    }

    Workload workload;
    workload.keys.reserve(config.key_count);
    workload.thread_operations.resize(config.thread_count);

    for (std::size_t i = 0; i < config.key_count; ++i)
    {
        workload.keys.push_back("key_" + std::to_string(i));
    }

    for (std::size_t worker_id = 0;
         worker_id < config.thread_count;
         ++worker_id)
    {
        // 各线程使用不同种子，但每次实验可重复生成相同负载。
        std::mt19937_64 generator(config.seed + worker_id);

        std::uniform_int_distribution<std::size_t> choose_key(
            0, config.key_count - 1
        );

        std::bernoulli_distribution choose_read(
            config.read_percent / 100.0
        );

        auto& operations = workload.thread_operations[worker_id];
        operations.reserve(config.operations_per_thread);

        for (std::size_t i = 0;
             i < config.operations_per_thread;
             ++i)
        {
            operations.push_back({
                choose_key(generator),
                choose_read(generator)
            });
        }
    }

    return workload;
}

template<typename Store>
BenchmarkResult run_benchmark(
    const BenchmarkConfig& config,
    const Workload& workload)
{
    Store store;

    const std::string initial_value(config.value_bytes, 'a');
    const std::string updated_value(config.value_bytes, 'b');

    // 数据初始化不计入操作耗时。
    for (const auto& key : workload.keys)
    {
        store.set(key, initial_value);
    }

    std::atomic<std::size_t> ready_count{0};
    std::atomic<bool> start{false};

    std::vector<WorkerStats> worker_stats(config.thread_count);
    std::vector<std::exception_ptr> worker_errors(config.thread_count);

    std::vector<std::thread> workers;
    workers.reserve(config.thread_count);

    try
    {
        for (std::size_t worker_id = 0;
             worker_id < config.thread_count;
             ++worker_id)
        {
            workers.emplace_back(
                [&, worker_id]()
                {
                    ready_count.fetch_add(1);

                    while (!start.load())
                    {
                        std::this_thread::yield();
                    }

                    try
                    {
                        WorkerStats local;

                        const auto& operations =
                            workload.thread_operations[worker_id];

                        for (const auto& operation : operations)
                        {
                            const auto& key =
                                workload.keys[operation.key_index];

                            if (operation.is_read)
                            {
                                const auto result = store.get(key);
                                ++local.reads;

                                if (!result.has_value() ||
                                    result->size() != config.value_bytes)
                                {
                                    ++local.errors;
                                    continue;
                                }

                                // 消费查询结果，结束后输出校验和。
                                local.checksum += result->size();
                                local.checksum +=
                                    static_cast<unsigned char>(
                                        result->front()
                                    );
                            }
                            else
                            {
                                store.set(key, updated_value);
                                ++local.writes;
                            }
                        }

                        // 执行期间只更新局部变量，结束时写回。
                        worker_stats[worker_id] = local;
                    }
                    catch (...)
                    {
                        worker_errors[worker_id] =
                            std::current_exception();
                    }
                }
            );
        }
    }
    catch (...)
    {
        // 如果线程创建失败，放行并回收已创建的线程。
        start.store(true);

        for (auto& worker : workers)
        {
            worker.join();
        }

        throw;
    }

    while (ready_count.load() != config.thread_count)
    {
        std::this_thread::yield();
    }

    using Clock = std::chrono::steady_clock;

    const auto begin = Clock::now();
    start.store(true);

    for (auto& worker : workers)
    {
        worker.join();
    }

    const auto end = Clock::now();

    // 检查、汇总不计入耗时。
   for (const auto& error : worker_errors)
    {
        if (error)
        {
            std::rethrow_exception(error);
        }
    }

    BenchmarkResult result;

    result.elapsed_seconds =
        std::chrono::duration<double>(end - begin).count();

    for (const auto& stats : worker_stats)
    {
        result.totals.reads += stats.reads;
        result.totals.writes += stats.writes;
        result.totals.errors += stats.errors;
        result.totals.checksum += stats.checksum;
    }

    if (result.totals.errors != 0)
    {
        throw std::runtime_error(
            "benchmark detected missing keys or invalid value sizes"
        );
    }

    // 本负载只覆盖已有键，因此键数量不应变化。
    if (store.size() != config.key_count)
    {
        throw std::runtime_error("unexpected store size");
    }

    if (result.elapsed_seconds <= 0.0)
    {
        throw std::runtime_error("invalid elapsed time");
    }

    return result;
}

void print_result(
    const char* implementation,
    const BenchmarkConfig& config,
    int repetition,
    const BenchmarkResult& result)
{
    const auto total_operations =
        result.totals.reads + result.totals.writes;

    const double throughput =
        static_cast<double>(total_operations) /
        result.elapsed_seconds;

    std::cout
        << implementation << ','
        << config.thread_count << ','
        << config.key_count << ','
        << config.value_bytes << ','
        << config.operations_per_thread << ','
        << config.read_percent << ','
        << config.seed << ','
        << repetition << ','
        << result.totals.reads << ','
        << result.totals.writes << ','
        << total_operations << ','
        << result.elapsed_seconds << ','
        << throughput << ','
        << result.totals.errors << ','
        << result.totals.checksum
        << '\n';
}

int main()
{
    try
    {
        const std::vector<std::size_t> thread_counts{1, 2, 4};
        const std::vector<unsigned int> read_percentages{100, 80, 50};
        const int repetitions = 5;

        std::cout << std::fixed << std::setprecision(6);

        std::cout
            << "implementation,threads,keys,value_bytes,"
            << "ops_per_thread,target_read_percent,seed,repeat,"
            << "reads,writes,total_ops,seconds,ops_per_second,"
            << "errors,checksum\n";

        for (const auto read_percent : read_percentages)
        {
            for (const auto thread_count : thread_counts)
            {
                BenchmarkConfig config;
                config.thread_count = thread_count;
                config.read_percent = read_percent;

                // 两个实现使用同一份负载。
                const auto workload = make_workload(config);

                // 预热，不输出结果。
                run_benchmark<single_lock_kv_store>(config, workload);
                run_benchmark<sharded_kv_store>(config, workload);

                // 封装单锁版本的一次测量。
                const auto measure_single_lock = [&](int repetition)
                {
                    const auto result =
                        run_benchmark<single_lock_kv_store>(
                            config, workload
                        );

                    print_result(
                        "single_lock",
                        config,
                        repetition,
                        result
                    );
                };

                // 封装分片版本的一次测量。
                const auto measure_sharded = [&](int repetition)
                {
                    const auto result =
                        run_benchmark<sharded_kv_store>(
                            config, workload
                        );

                    print_result(
                        "sharded",
                        config,
                        repetition,
                        result
                    );
                };

                for (int repetition = 1;
                     repetition <= repetitions;
                     ++repetition)
                {
                    // 每轮交替执行顺序。
                    if (repetition % 2 == 1)
                    {
                        measure_single_lock(repetition);
                        measure_sharded(repetition);
                    }
                    else
                    {
                        measure_sharded(repetition);
                        measure_single_lock(repetition);
                    }
                }
            }
        }

        return 0;
    }
    catch (const std::exception& error)
    {
        std::cerr << "benchmark failed: " << error.what() << '\n';
        return 1;
    }
}