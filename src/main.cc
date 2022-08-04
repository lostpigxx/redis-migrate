#include <iostream>
#include <thread>
#include <list>
#include <string>
#include <mutex>
#include <atomic>
#include <chrono>
#include <vector>

#include <hiredis/hiredis.h>

#include <zlib.h>
#include <zlib.h>

using namespace std::chrono;

class RestoreItem {
public:
    RestoreItem() : ttl_(0) {}

    bool Empty() {
        return key_.empty() && ttl_ == 0 && val_.empty();
    }

public:
    std::string key_;
    uint64_t ttl_;
    std::string val_;
};

class LockedList {
public:
    LockedList() : is_producer_done_(false) {}

    void Push(RestoreItem&& str) {
        std::lock_guard<std::mutex> lck(mu_);
        list_.emplace_back(str);
    }

    RestoreItem Pop() {
        std::lock_guard<std::mutex> lck(mu_);
        if (list_.empty()) {
            return RestoreItem();
        }
        RestoreItem ret = std::move(list_.front());
        list_.pop_front();
        return ret;
    }

    std::size_t Size() {
        std::lock_guard<std::mutex> lck(mu_);
        return list_.size();
    }

    void Done() {
        is_producer_done_.store(true);
    }

    bool IsProducerDone() {
        return is_producer_done_.load();
    }

private:
    std::mutex mu_;
    std::list<RestoreItem> list_;
    std::atomic_bool is_producer_done_;
};

class RedisServer {
public:
    RedisServer(const std::string& ip, int port, const std::string& pwd, uint32_t min, uint32_t max)
        : ip_(ip), port_(port), pwd_(pwd), slot_min_(min), slot_max_(max), redis_(nullptr) {}

    void Connect() {
        // std::cout << "Connect to " << ip_ << " " << port_ << std::endl;
        redis_ = redisConnect(ip_.c_str(), port_);
        if (pwd_.empty()) {
            return;
        }
        redisReply* r = nullptr;
        r = (redisReply*)redisCommand(redis_, "auth %b", pwd_.data(), pwd_.size());
        if (std::string(r->str, r->len) != "OK") {
            std::cout << "Redis server auth failed" << std::endl;
            exit(0);
        }
        freeReplyObject(r);
    }
    ~RedisServer() {
        if (redis_ != nullptr) {
            redisFree(redis_);
        }
    }
public:
    std::string ip_;
    int port_;
    std::string pwd_;
    uint32_t slot_min_;
    uint32_t slot_max_;  // [slot_min_, slot_max_]
    redisContext* redis_;
};

class CodisCluster {
public:
    CodisCluster(uint32_t max_slot) : slot_num_(max_slot) {}
    void Init() {
        for (auto& svr : svrs_) {
            svr.Connect();
        }
    }
    redisContext* FindSever(uint32_t slot) {
        // TODO: 朴素算法，server比较少的时候够用了，可优化
        for (auto i = 0u; i < svrs_.size(); ++i) {
            if (slot >= svrs_[i].slot_min_ && slot <= svrs_[i].slot_max_) {
                return svrs_[i].redis_;
            }
        }
        std::cout << "Cannot find slot server " << slot << std::endl;
        exit(0);
        return svrs_[0].redis_;  // should never reach here
    }
public:
    uint32_t slot_num_;
    std::vector<RedisServer> svrs_;
};

constexpr std::size_t MAX_LIST_LEN = 10000;

uint32_t CalcSlot(const std::string& key, uint32_t max_slot) {
    auto crc = crc32(0L, Z_NULL, 0);
    return crc32(crc, (const unsigned char*)key.data(), key.size()) % max_slot;
}

void ScanThread(RedisServer src_svr, std::vector<LockedList>& lists) {
    src_svr.Connect();

    uint64_t cnt = 0;
    int list_index = 0;
    std::string cursor = "0";
    while (true) {
        redisReply* r = nullptr;
        r = (redisReply*)redisCommand(src_svr.redis_, "scan %b count 100", cursor.data(), cursor.size());
        cursor = std::string(r->element[0]->str, r->element[0]->len);

        for (auto i = 0u; i < r->element[1]->elements; ++i) {
            RestoreItem item;
            item.key_ = std::string(r->element[1]->element[i]->str, r->element[1]->element[i]->len);

            // 避免任务堆积过多导致OOM
            while (lists[list_index].Size() > MAX_LIST_LEN) {
                list_index = (list_index + 1) % lists.size();
            }
            lists[list_index].Push(std::move(item));

            ++cnt;
            if (cnt % 10000 == 0) {
                std::cout << "Scan thread count: " << cnt << std::endl;
            }
        }
        freeReplyObject(r);

        if (cursor == "0") {
            break;
        }
    }

    for (auto& list : lists) {
        list.Done();
    }
}

void WorkerThread(RedisServer src_svr, CodisCluster codis, std::vector<LockedList>& lists) {
    std::vector<std::thread> threads;
    for (auto i = 0u; i < lists.size(); ++i) {
        threads.emplace_back([](RedisServer src_svr, CodisCluster codis, std::size_t id, LockedList& list) {
            src_svr.Connect();
            codis.Init();
            uint64_t cnt = 0;
            while (!list.IsProducerDone() || list.Size() > 0) {
                auto item = list.Pop();
                if (item.Empty()) {
                    std::this_thread::sleep_for(100ms);
                    continue;
                }

                redisReply* r = nullptr;
                // dump
                r = (redisReply*)redisCommand(src_svr.redis_, "dump %b", item.key_.data(), item.key_.size());
                item.val_ = std::string(r->str, r->len);
                freeReplyObject(r);

                // ttl
                r = (redisReply*)redisCommand(src_svr.redis_, "ttl %b", item.key_.data(), item.key_.size());
                if (r->integer < 0) {
                    item.ttl_ = 0;  // restore命令中ttl为0表示不过期
                } else {
                    item.ttl_ = r->integer;
                }
                freeReplyObject(r);

                auto dst_redis = codis.FindSever(CalcSlot(item.key_, codis.slot_num_));
                // restore
                r = (redisReply*)redisCommand(dst_redis, "restore %b %llu %b REPLACE",
                        item.key_.data(), item.key_.size(), item.ttl_, item.val_.data(), item.val_.size());
                freeReplyObject(r);

                ++cnt;
                if (cnt % 10000 == 0) {
                    std::cout << "WorkerThread " << id << " count: " << cnt << std::endl;
                }
            }
        }, src_svr, codis, i, std::ref(lists[i]));
    }

    for (auto& thread : threads) {
        thread.join();
    }
}

//
int main(int argc, char** argv) {
    // TODO: 通过输入获取
    RedisServer src_redis = {"127.0.0.1", 6379, "a", 0, 0};

    CodisCluster codis(1024);
    codis.svrs_.emplace_back(RedisServer("127.0.0.1", 6380, "a", 0, 300));
    codis.svrs_.emplace_back(RedisServer("127.0.0.1", 6380, "a", 301, 600));
    codis.svrs_.emplace_back(RedisServer("127.0.0.1", 6380, "a", 601, 900));
    codis.svrs_.emplace_back(RedisServer("127.0.0.1", 6380, "a", 901, 1023));

    int N = 32;
    std::vector<LockedList> lists(N);

    std::thread scan_thread(ScanThread, src_redis, std::ref(lists));
    std::thread worker_thread(WorkerThread, src_redis, codis, std::ref(lists));

    scan_thread.join();
    worker_thread.join();
}