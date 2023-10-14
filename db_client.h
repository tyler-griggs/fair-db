#ifndef DB_CLIENT_H
#define DB_CLIENT_H

#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "atomicops.h"
#include "readerwriterqueue.h"

using namespace moodycamel;
using namespace std;

// TODO: Add internal metric state

// TODO: create client arguments to modify request size, type, frequency, etc.
struct SingleRead {
  size_t start_idx;
  size_t read_size;
  size_t compute_duration = 0;

  SingleRead(size_t start_idx, size_t read_size, size_t compute_duration = 0)
      : start_idx(start_idx), read_size(read_size),
        compute_duration(compute_duration) {}
};

struct DBRequest {
  int client_id;
  std::vector<SingleRead> reads;

  DBRequest(int client_id, std::vector<SingleRead> reads)
      : client_id(client_id), reads(reads) {}
};

class DBClient {
public:
  DBClient(const int client_id, ReaderWriterQueue<DBRequest> *request_queue,
           const size_t db_size_elements, const size_t read_size,
           const size_t compute_duration_ms)
      : client_id_(client_id), request_queue_(request_queue),
        db_size_elements_(db_size_elements), read_size_(read_size),
        compute_duration_ms_(compute_duration_ms) {}

  std::thread RunSequential(std::atomic<bool>& stop) {
    srand(time(0));
    cout << "Client ID " << client_id_ << " running Sequential of "
         << read_size_ / 1e6 << "M elements." << endl;
    std::thread client_thread([this, &stop] {

      while (!stop.load()) {
        int start_idx = rand() % (db_size_elements_ - read_size_);
        std::vector<SingleRead> req(
            1, SingleRead(start_idx, read_size_, compute_duration_ms_));

        while (!request_queue_->try_enqueue(DBRequest(client_id_, req)) && !stop.load()) {
          std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
      }
      cout << "Client ID " << client_id_ << " completed." << endl;
    });
    return client_thread;
  }

  std::thread RunRandom(std::atomic<bool>& stop) {
    srand(time(0));
    cout << "Client ID " << client_id_ << " running Random of "
         << read_size_ / 1e6 << "M elements." << endl;
    std::thread client_thread([this, &stop] {
      size_t num_random = 1024;

      while (!stop.load()) {
        std::vector<SingleRead> reqs;
        for (int i = 0; i < num_random; ++i) {
          int start_idx =
              rand() % (db_size_elements_ - read_size_ / num_random);
          reqs.push_back(SingleRead(start_idx, read_size_ / num_random));
        }

        while (!request_queue_->try_enqueue(DBRequest(client_id_, reqs)) && !stop.load()) {
          std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
      }
      cout << "Client ID " << client_id_ << " completed." << endl;
    });
    return client_thread;
  }

private:
  const int client_id_;
  ReaderWriterQueue<DBRequest> *request_queue_;
  const size_t db_size_elements_;
  const size_t read_size_;
  const size_t compute_duration_ms_;
};
#endif