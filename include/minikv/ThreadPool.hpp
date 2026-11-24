#pragma once

#include "blocking_queue.hpp"

#include <cstddef>
#include <functional>
#include <thread>
#include <vector>
#include <iostream>
class ThreadPool
{
public:
    using Task = std::function<void()>;

    ThreadPool(std::size_t worker_count,
               std::size_t queue_capacity)
            :tasks_(queue_capacity)
        {   
            if(worker_count == 0)
            {
                throw std::invalid_argument("0 worker");
            }
            workers_.reserve(worker_count);
            try{
            for(size_t i = 0 ; i < worker_count ; i++)
                {
                    workers_.emplace_back(
                    [this]
                    {this->worker_loop();
                    }
                    );
                }
            }
            catch(...)
            {
                shutdown();
                throw;
            }
        }
    

    ~ThreadPool()
    {
        shutdown();
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    PushResult try_submit(Task task)
    {
        if(task != nullptr)
        {
            return tasks_.try_push(task);
        }
        throw std::invalid_argument("empty task");
    }

    void shutdown()
    {
        tasks_.close();
        for (auto& worker : workers_)
        {
            if (worker.joinable())
            {
                worker.join();
            }
        }
    }

private:
    void worker_loop()
    {
        while(true)
        {
            std::optional<Task> task = tasks_.wait_pop();
            if(task == std::nullopt)
            {   
                break;
            }
            try 
            {
                (*task)();
            }
            catch(std::exception& ex)
            {
                 std::cerr << "task failed: " << ex.what() << '\n';
            }
            catch(...)
            {
                 std::cerr << "task failed: unknown exception\n";
            }
        }
    }

    BlockingQueue<Task> tasks_;
    std::vector<std::thread> workers_;
};