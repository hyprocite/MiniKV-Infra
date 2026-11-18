#include<optional>
#include<string>
#include<unordered_map>
#include<cstddef>
#include<shared_mutex>
#include<mutex>
class kv_store
{

    public:
    
    void set(const std::string&  key,const std::string& value)
    {   
        std::unique_lock lock(mtx);
        m_store[key] = value;
    }
    
    std::optional<std::string> get (std::string key) const
    {
        std::shared_lock lock(mtx);
        auto value = m_store.find(key);
        if(value == m_store.end())
        {
            return {};
        }
        return value->second;
    }
    
    bool del(std::string key)
    {   
        std::unique_lock lock(mtx);
        return m_store.erase(key)>0;
    }

    std::size_t size() const
    {   
        std::shared_lock lock(mtx);
        return m_store.size();
    }

    





    private:
    std::unordered_map<std::string,std::string> m_store;
    mutable std::shared_mutex mtx;

   


};