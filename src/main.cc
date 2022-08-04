#include <iostream>
#include <thread>
#include <list>
#include <string>
#include <mutex>
#include <atomic>
#include <chrono>
#include <vector>

#include <hiredis/hiredis.h>

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
    std::string ip_;
    int port_;
    std::string pwd_;
};

constexpr std::size_t MAX_LIST_LEN = 10000;

void ScanThread(RedisServer& src_svr, std::vector<LockedList>& lists) {
    redisContext* redis = redisConnect(src_svr.ip_.c_str(), src_svr.port_);
    redisReply* r = nullptr;

    r = (redisReply*)redisCommand(redis, "auth %b", src_svr.pwd_.data(), src_svr.pwd_.size());
    std::cout << r->str << std::endl;
    freeReplyObject(r);

    uint64_t cnt = 0;
    int list_index = 0;
    std::string cursor = "0";
    while (true) {
        r = (redisReply*)redisCommand(redis, "scan %b count 100", cursor.data(), cursor.size());
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

    redisFree(redis);
}

void WorkerThread(RedisServer& src_svr, RedisServer& dst_svr, std::vector<LockedList>& lists) {
    std::vector<std::thread> threads;
    for (auto i = 0u; i < lists.size(); ++i) {
        threads.emplace_back([&](std::size_t id, LockedList& list) {
            redisContext* src_redis = redisConnect(src_svr.ip_.c_str(), src_svr.port_);
            redisContext* dst_redis = redisConnect(dst_svr.ip_.c_str(), dst_svr.port_);
            redisReply* r = nullptr;

            r = (redisReply*)redisCommand(src_redis, "auth %b", src_svr.pwd_.data(), src_svr.pwd_.size());
            freeReplyObject(r);
            r = (redisReply*)redisCommand(dst_redis, "auth %b", dst_svr.pwd_.data(), dst_svr.pwd_.size());
            freeReplyObject(r);

            uint64_t cnt = 0;
            while (!list.IsProducerDone() || list.Size() > 0) {
                auto item = list.Pop();
                if (item.Empty()) {
                    std::this_thread::sleep_for(100ms);
                    continue;
                }

                // dump
                r = (redisReply*)redisCommand(src_redis, "dump %b", item.key_.data(), item.key_.size());
                item.val_ = std::string(r->str, r->len);
                freeReplyObject(r);

                // ttl
                r = (redisReply*)redisCommand(src_redis, "ttl %b", item.key_.data(), item.key_.size());
                if (r->integer < 0) {
                    item.ttl_ = 0;  // restore命令中ttl为0表示不过期
                } else {
                    item.ttl_ = r->integer;
                }
                freeReplyObject(r);

                // restore
                r = (redisReply*)redisCommand(dst_redis, "restore %b %llu %b REPLACE",
                        item.key_.data(), item.key_.size(), item.ttl_, item.val_.data(), item.val_.size());
                freeReplyObject(r);

                ++cnt;
                if (cnt % 10000 == 0) {
                    std::cout << "WorkerThread " << id << " count: " << cnt << std::endl;
                }
            }

            redisFree(src_redis);
            redisFree(dst_redis);
        }, i, std::ref(lists[i]));
    }

    for (auto& thread : threads) {
        thread.join();
    }
}


int main(int argc, char** argv) {
    // TODO: 通过输入获取
    RedisServer src_redis = {"127.0.0.1", 6379, "a"};
    RedisServer dst_redis = {"127.0.0.1", 6380, "a"};

    int N = 16;
    std::vector<LockedList> lists(N);

    std::thread scan_thread(ScanThread, std::ref(src_redis), std::ref(lists));
    std::thread worker_thread(WorkerThread, std::ref(src_redis), std::ref(dst_redis), std::ref(lists));

    scan_thread.join();
    worker_thread.join();
}