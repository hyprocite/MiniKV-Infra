#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>

enum class PushResult
{
    Accepted,
    Full,
    Closed
};

template<typename T>
class BlockingQueue
{
public:
    explicit BlockingQueue(std::size_t capacity)
    :capacity_(capacity)
    {
    }

    PushResult try_push(T value)
    {
        {
            std::lock_guard<std::mutex> lock_(mutex_);
            if(closed_)
            {
                return PushResult::Closed;
            }
            if(queue_.size() >= capacity_)
            {
                return PushResult::Full;
            }
            queue_.push_back(std::move(value));       
         }
        //出作用域自动放锁
        not_empty_.notify_one();
        return PushResult::Accepted;
    }

    std::optional<T> wait_pop()
    {
            std::unique_lock<std::mutex> lock_(mutex_);
            not_empty_.wait(lock_,[this]
            {
                return closed_ || !queue_.empty();
            });
            if (queue_.empty())
            {
                // 等待条件已经成立，又没有元素，
                // 因此此时必然是已关闭且排空。
                return std::nullopt;
            }
            T value = std::move(queue_.front());
            queue_.pop_front();
            return std::optional<T>{std::move(value)}; 
    }

    void close()
    {
        {
        std::lock_guard<std::mutex> lock_(mutex_);
        closed_ = true;
        }
        not_empty_.notify_all();
    }

    std::size_t size() const;

private:
    const std::size_t capacity_;
    std::deque<T> queue_;

    bool closed_ = false;

    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
};