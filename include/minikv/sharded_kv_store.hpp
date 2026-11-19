#include<optional>
#include<string>
#include<unordered_map>
#include<cstddef>
#include<shared_mutex>
#include<mutex>
#include<array>
#include<functional>
class sharded_kv_store
{

    private:


    static constexpr std::size_t shard_count = 16;
    
    struct Shard
    {
        std::unordered_map<std::string,std::string> data;
          mutable std::shared_mutex mtx;
    };
    
    std::array<Shard,shard_count> shards_;

    size_t shard_index(const std::string& key) const
    {
        return std::hash<std::string>{}(key) %shard_count;
    }


    public:
    
    void set(const std::string&  key,const std::string& value)
    {   
        auto& shard = shards_[shard_index(key)];
        std::unique_lock lock(shard.mtx);
        shard.data[key] = value;
    }
    
    std::optional<std::string> get (std::string key) const
    {
        auto& shard = shards_[shard_index(key)];
        std::shared_lock lock(shard.mtx);
        auto value = shard.data.find(key);
        if(value == shard.data.end())
        {
            return {};
        }
        return value->second;
    }
    
    bool del(std::string key)
    {   
        auto& shard = shards_[shard_index(key)];
        std::unique_lock lock(shard.mtx);
        return shard.data.erase(key)>0;
    }

    std::size_t size() const
    {   
        std::size_t count = 0;

        std::array <std::shared_lock<std::shared_mutex> ,shard_count>  lock;
        for(int i = 0 ; i< shard_count ; i++)
        {
            lock[i] = std::shared_lock<std::shared_mutex>(shards_[i].mtx);
        }
        
        for(const auto& shard : shards_)
        {
            count+=shard.data.size();
        }
        return count;
    }

    





};