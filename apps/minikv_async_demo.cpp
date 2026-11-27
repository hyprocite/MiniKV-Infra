#include"../include/minikv/ThreadPool.hpp"
#include"../include/minikv/sharded_kv_store.hpp"
#include<vector>
#include<string>
#include<iostream>
int main()
{
    size_t woker_count = 4;
    size_t queue_capacity = 100;
    ThreadPool thread_pool_(woker_count,queue_capacity);
    sharded_kv_store kv_store_;
    std::vector<int> results(100,0);
    std::vector<PushResult> pushresult(100); 
    for(int i = 0 ; i< 100 ; i++)
    {   
        const std::string suffix = std::to_string(i);
        const std::string key = "key_" + suffix;
        const std::string value =  "value_" + suffix;
        pushresult[i] = thread_pool_.try_submit([&kv_store_,&results,i,key,value]
        {
            kv_store_.set(key,value);
            const auto result = kv_store_.get(key);
            results[i] = result.value() == value ? 1:-1;
        }
        );
    }
    
    thread_pool_.shutdown();
    int accepted_tasks = 0;
    int successful_tasks = 0;

    for(int i = 0 ; i < 100 ;i++)
    {
        if(results[i] == 1)  successful_tasks++;
        if(pushresult[i] ==  PushResult::Accepted) accepted_tasks++;
    }
    if (accepted_tasks != 100 ||
        successful_tasks != 100 ||
        kv_store_.size() != 100)
    {
        std::cerr << "integration failed\n";
        return 1;
    }

    return 0;
}
