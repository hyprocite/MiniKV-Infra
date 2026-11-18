#include "../include/minikv/kv_store.hpp"

#include <cassert>
#include <cstddef>  
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <atomic>

void test_rw_test()
{
    kv_store store;
    const int thread_count = 2;
    const int keys_per_thread = 500;
    const int total_worker = thread_count*2;
    std::atomic<int> ready_count{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> read_workers;
    read_workers.reserve(thread_count);

    std::vector<std::thread> write_workers;
    write_workers.reserve(thread_count);

    for(int i = 0 ; i<thread_count*keys_per_thread ; i++)
    {
        const std::string key = "key_"+ std::to_string(i);
        const std::string value = "initial";
        store.set(key,value);
    }
    for(int worker_id = 0; worker_id < thread_count; ++worker_id)
    {   
        write_workers.emplace_back(
            [&store, worker_id, keys_per_thread,&ready_count, &start]()
            {  
                 ready_count.fetch_add(1);
                while (!start.load())
                {
                    std::this_thread::yield();
                }
                

                for (int i = worker_id*keys_per_thread; i < (worker_id+1)*keys_per_thread; ++i)
                {
                    const std::string suffix = std::to_string(i);

                    const std::string key = "key_" + suffix;
                    const std::string value = "value_" + suffix;

                    store.set(key, value);
                }
            }
        );
    }




    for (int worker_id = 0; worker_id < thread_count; ++worker_id)
    {
        read_workers.emplace_back(
            [&store, keys_per_thread, thread_count,&ready_count, &start]()
            {
                ready_count.fetch_add(1);
                while (!start.load())
                {
                    std::this_thread::yield();
                }
                
                for (int i = 0; i < thread_count*keys_per_thread; ++i)
                {
                    const std::string suffix = std::to_string(i);
                    const std::string key = "key_" + suffix;
                    const std::string updated_value = "value_" + suffix;
                    const auto result = store.get(key);
                    assert(result.has_value());
                    assert(result.value() == "initial" ||
                    result.value() == updated_value);
                }
            }
        );
    }
    const int total_workers = thread_count * 2;
    while (ready_count.load() != total_workers)
    {
        std::this_thread::yield();
    }

    start.store(true);

    for(int i = 0 ; i < thread_count; i++)
    {
        write_workers[i].join();
        read_workers[i].join();
    }

    std::size_t expected_size = std::size_t(thread_count * keys_per_thread);
    assert(store.size() == expected_size);
    for(int i = 0 ; i< expected_size; i++)
    {
        const std::string suffix = std::to_string(i);
        const std::string key = "key_" + suffix;
        const std::string expected_value = "value_" + suffix;
        auto result  = store.get(key);
        
        assert(result.has_value());
        assert(result.value() == expected_value);
    }

    std::cout << "test passed\n";
}

int main()
{
    test_rw_test();
    return 0;
}