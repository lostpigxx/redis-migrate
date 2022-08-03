#include <iostream>
#include <thread>
#include <list>
#include <string>
#include <mutex>
#include <atomic>
#include <chrono>

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

using RedisServer = std::pair<std::string, int>;

constexpr std::size_t MAX_LIST_LEN = 10000;

void ScanThread(RedisServer& server, LockedList& list_1) {
    redisContext* redis = redisConnect(server.first.c_str(), server.second);
    redisReply* r = nullptr;

    r = (redisReply*)redisCommand(redis, "auth a");
    std::cout << r->str << std::endl;
    freeReplyObject(r);

    int cnt = 0;
    std::string cursor = "0";
    while (true) {
        r = (redisReply*)redisCommand(redis, "scan %b count 100", cursor.data(), cursor.size());
        cursor = std::string(r->element[0]->str, r->element[0]->len);

        for (auto i = 0u; i < r->element[1]->elements; ++i) {
            RestoreItem item;
            item.key_ = std::string(r->element[1]->element[i]->str, r->element[1]->element[i]->len);
            list_1.Push(std::move(item));
            ++cnt;
            if (cnt % 10000 == 0) {
                std::cout << "Scan thread count: " << cnt << std::endl;
            }
        }
        freeReplyObject(r);

        if (list_1.Size() > MAX_LIST_LEN) {
            std::this_thread::sleep_for(100ms);
        }

        if (cursor == "0") {
            break;
        }
    }

    std::cout << cnt << std::endl;
    list_1.Done();

    redisFree(redis);
}

void DumpAndTtlThead(RedisServer& server, LockedList& list_1, LockedList& list_2) {
    redisContext* redis = redisConnect(server.first.c_str(), server.second);
    redisReply* r = nullptr;

    r = (redisReply*)redisCommand(redis, "auth a");
    std::cout << r->str << std::endl;
    freeReplyObject(r);

    int cnt = 0;
    while (!list_1.IsProducerDone() || list_1.Size() > 0) {
        auto item = list_1.Pop();
        if (item.Empty()) {
            std::this_thread::sleep_for(1s);
            continue;
        }

        // dump
        r = (redisReply*)redisCommand(redis, "dump %b", item.key_.data(), item.key_.size());
        item.val_ = std::string(r->str, r->len);
        freeReplyObject(r);
        // ttl
        r = (redisReply*)redisCommand(redis, "ttl %b", item.key_.data(), item.key_.size());
        if (r->integer < 0) {
            item.ttl_ = 0;  // restore命令中ttl为0表示不过期
        } else {
            item.ttl_ = r->integer;
        }
        freeReplyObject(r);

        list_2.Push(std::move(item));
        if (list_2.Size() > MAX_LIST_LEN) {
            std::this_thread::sleep_for(100ms);
        }

        ++cnt;
        if (cnt % 10000 == 0) {
            std::cout << "Dump and ttl thread count: " << cnt << std::endl;
        }
    }

    list_2.Done();
    redisFree(redis);
}

void RestoreThread(RedisServer& server, LockedList& list_2) {
    redisContext* redis = redisConnect(server.first.c_str(), server.second);
    redisReply* r = nullptr;

    r = (redisReply*)redisCommand(redis, "auth a");
    std::cout << r->str << std::endl;
    freeReplyObject(r);

    int cnt = 0;
    while (!list_2.IsProducerDone() || list_2.Size() > 0) {
        auto item = list_2.Pop();
        if (item.Empty()) {
            std::this_thread::sleep_for(1s);
            continue;
        }

        r = (redisReply*)redisCommand(redis, "restore %b %llu %b REPLACE",
            item.key_.data(), item.key_.size(), item.ttl_, item.val_.data(), item.val_.size());
        freeReplyObject(r);

        ++cnt;
        if (cnt % 10000 == 0) {
            std::cout << "Restore thread count: " << cnt << std::endl;
        }
    }

    redisFree(redis);
}

//
// 1 scan thread
// N dump&ttl thread
// M restore thread
int main(int argc, char** argv) {
    // TODO: 通过输入获取
    RedisServer src_redis = {"127.0.0.1", 6379};
    RedisServer dest_redis = {"127.0.0.1", 6380};

    LockedList list_1;  // scan_thread -> dump_ttl_thread
    LockedList list_2;  // dump_ttl_thread -> restore_thread
    std::thread scan_thread(ScanThread, std::ref(src_redis), std::ref(list_1));
    std::this_thread::sleep_for(1s);
    std::thread dump_ttl_thread(DumpAndTtlThead, std::ref(src_redis), std::ref(list_1), std::ref(list_2));
    std::this_thread::sleep_for(1s);
    std::thread restore_thread(RestoreThread, std::ref(dest_redis), std::ref(list_2));

    scan_thread.join();
    dump_ttl_thread.join();
    restore_thread.join();
}