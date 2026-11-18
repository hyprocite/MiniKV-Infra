#include "../include/minikv/kv_store.hpp"

#include <cassert>
#include <cstddef>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

void test_concurrent_set()
{
    kv_store store;

    const int thread_count = 4;
    const int keys_per_thread = 1000;

    std::vector<std::thread> workers;
    workers.reserve(thread_count);

    // 先创建所有线程。
    for (int worker_id = 0; worker_id < thread_count; ++worker_id)
    {
        workers.emplace_back(
            [&store, worker_id, keys_per_thread]()
            {
                for (int i = 0; i < keys_per_thread; ++i)
                {
                    const std::string suffix =
                        std::to_string(worker_id) + "_" + std::to_string(i);

                    const std::string key = "worker_" + suffix;
                    const std::string value = "value_" + suffix;

                    store.set(key, value);
                }
            }
        );
    }

    // 所有线程启动后，再逐个等待结束。
    for (auto& worker : workers)
    {
        worker.join();
    }

    const auto expected_size =
        static_cast<std::size_t>(thread_count) * keys_per_thread;

    assert(store.size() == expected_size);

    // 不只检查数量，还检查每一个键及其对应的值。
    for (int worker_id = 0; worker_id < thread_count; ++worker_id)
    {
        for (int i = 0; i < keys_per_thread; ++i)
        {
            const std::string suffix =
                std::to_string(worker_id) + "_" + std::to_string(i);

            const std::string key = "worker_" + suffix;
            const std::string expected_value = "value_" + suffix;

            const auto result = store.get(key);

            assert(result.has_value());
            assert(result.value() == expected_value);
        }
    }

    std::cout << "test_concurrent_set passed\n";
}

int main()
{
    test_concurrent_set();
    return 0;
}