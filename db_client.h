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
           const size_t client_timeout_seconds,
           const size_t compute_duration_ms)
      : client_id_(client_id), request_queue_(request_queue),
        db_size_elements_(db_size_elements), read_size_(read_size),
        client_timeout_seconds_(client_timeout_seconds),
        compute_duration_ms_(compute_duration_ms) {}

  std::thread RunSequential() {
    srand(time(0));
    cout << "Client ID " << client_id_ << " running Sequential of "
         << read_size_ / 1e6 << "M elements." << endl;
    std::thread client_thread([this] {
      auto timeout = std::chrono::seconds(client_timeout_seconds_);
      auto start_time = std::chrono::steady_clock::now();

      while (std::chrono::steady_clock::now() - start_time < timeout) {
        int start_idx = rand() % (db_size_elements_ - read_size_);
        std::vector<SingleRead> req(
            1, SingleRead(start_idx, read_size_, compute_duration_ms_));

        while (!request_queue_->try_enqueue(DBRequest(client_id_, req)) &&
               std::chrono::steady_clock::now() - start_time < timeout) {
          std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
      }
      cout << "Client ID " << client_id_ << " completed." << endl;
    });
    return client_thread;
  }

  std::thread RunRandom() {
    srand(time(0));
    cout << "Client ID " << client_id_ << " running Random of "
         << read_size_ / 1e6 << "M elements." << endl;
    std::thread client_thread([this] {
      auto timeout = std::chrono::seconds(client_timeout_seconds_);
      auto start_time = std::chrono::steady_clock::now();
      size_t num_random = 1024;

      while (std::chrono::steady_clock::now() - start_time < timeout) {
        std::vector<SingleRead> reqs;
        for (int i = 0; i < num_random; ++i) {
          int start_idx =
              rand() % (db_size_elements_ - read_size_ / num_random);
          reqs.push_back(SingleRead(start_idx, read_size_ / num_random));
        }

        while (!request_queue_->try_enqueue(DBRequest(client_id_, reqs)) &&
               std::chrono::steady_clock::now() - start_time < timeout) {
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
  const size_t client_timeout_seconds_;
  const size_t compute_duration_ms_;
};
#endif