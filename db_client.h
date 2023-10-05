#ifndef DB_CLIENT_H
#define DB_CLIENT_H

#include <iostream>
#include <chrono>
#include <thread>
#include <vector>

#include "readerwriterqueue.h"
#include "atomicops.h"

using namespace moodycamel;
using namespace std;

// TODO: if try_enqueue fails, need to try again 
// TODO: update all member variables to have _ suffix
// TODO: add internaal metric state

// TODO: create client arguments to modify request size, type, frequency, etc.
struct SingleRead {
    size_t start_idx;
    size_t read_size;

    SingleRead(size_t start_idx, size_t read_size) : start_idx(start_idx), read_size(read_size) {}
};

struct DBRequest {
    int client_id;
    std::vector<SingleRead> reads;

    DBRequest(int client_id, std::vector<SingleRead> reads) : client_id(client_id), reads(reads) {}
};

class DBClient {
public:
    DBClient(const int client_id, ReaderWriterQueue<DBRequest>* request_queue, const size_t db_size_elements, const size_t read_size, const size_t num_reads) : 
    client_id(client_id),
    request_queue_(request_queue), 
    db_size_elements(db_size_elements),
    read_size(read_size),
    num_reads(num_reads)  {}

    std::thread RunSequential() {
        srand(time(0));
        cout << "Client ID " << client_id << " running Sequential." << endl;
        std::thread client_thread(RunSequentialInternal, client_id, request_queue_, db_size_elements, read_size, num_reads);
        return client_thread;
    }

    std::thread RunRandom() {
        srand(time(0));
        cout << "Client ID " << client_id << " running Random." << endl;
        std::thread client_thread(RunRandomInternal, client_id, request_queue_, db_size_elements, read_size, num_reads);
        return client_thread;
    }

private:
    const int client_id;
    ReaderWriterQueue<DBRequest>* request_queue_;
    const size_t db_size_elements;
    const size_t read_size;
    const size_t num_reads;

    static void RunSequentialInternal(const int client_id, ReaderWriterQueue<DBRequest>* request_queue, const size_t db_size_elements, const size_t read_size, const size_t num_reads) {
        for (int i = 0; i < num_reads; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));  // 1kqps

            int start_idx = rand() % (db_size_elements - read_size);
            std::vector<SingleRead> req(1, SingleRead(start_idx, read_size));
            while (!request_queue->try_enqueue(DBRequest(client_id, req))) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
        cout << "Client ID " << client_id << " completed." << endl;
    }

    static void RunRandomInternal(const int client_id, ReaderWriterQueue<DBRequest>* request_queue, const size_t db_size_elements, const size_t read_size, const size_t num_reads) {
        size_t num_random = 1024;
        for (int i = 0; i < num_reads; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));  // 1kqps
            std::vector<SingleRead> reqs;
            for (int i = 0; i < num_random; ++i) {
                int start_idx = rand() % (db_size_elements - read_size / num_random);
                reqs.push_back(SingleRead(start_idx, read_size / num_random));
            }
            while (!request_queue->try_enqueue(DBRequest(client_id, reqs))) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
        cout << "Client ID " << client_id << " completed." << endl;
    }
};
#endif