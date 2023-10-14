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

// A single read request for the database to serve.
// TODO: Allow clients to make more flexible queries.
struct SingleRead {
  size_t start_idx;
  size_t read_size;
  size_t compute_duration = 0;

  SingleRead(size_t start_idx, size_t read_size, size_t compute_duration = 0)
      : start_idx(start_idx), read_size(read_size),
        compute_duration(compute_duration) {}
};

// A set of read requests for the database to serve.
struct DBRequest {
  int client_id;
  std::vector<SingleRead> reads;

  DBRequest(int client_id, std::vector<SingleRead> reads)
      : client_id(client_id), reads(reads) {}
};

class DBClient {
public:
  DBClient(int client_id,
           const std::shared_ptr<ReaderWriterQueue<DBRequest>> &request_queue,
           size_t db_size_elements, size_t read_size,
           size_t compute_duration_ms)
      : client_id_(client_id), request_queue_(request_queue),
        db_size_elements_(db_size_elements), read_size_(read_size),
        compute_duration_ms_(compute_duration_ms) {}

  // Run the sequential read workload.
  std::thread RunSequential(std::atomic<bool> &stop) {
    srand(time(0));
    cout << "Client ID " << client_id_ << " running Sequential workload of "
         << read_size_ / 1e6 << "M elements." << endl;
    std::thread client_thread([this, &stop] {
      while (!stop.load()) {
        int start_idx = rand() % (db_size_elements_ - read_size_);
        std::vector<SingleRead> req{
            SingleRead(start_idx, read_size_, compute_duration_ms_)};

        while (!request_queue_->try_enqueue(DBRequest(client_id_, req)) &&
               !stop.load()) {
          std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
      }
      cout << "Client ID " << client_id_ << " completed." << endl;
    });
    return client_thread;
  }

  // Run the random read workload.
  std::thread RunRandom(std::atomic<bool> &stop, int num_random) {
    srand(time(0));
    cout << "Client ID " << client_id_ << " running Random workload  of "
         << read_size_ / 1e6 << "M elements." << endl;
    std::thread client_thread([this, &stop, num_random] {
      while (!stop.load()) {
        std::vector<SingleRead> reqs;
        for (int i = 0; i < num_random; ++i) {
          int start_idx =
              rand() % (db_size_elements_ - read_size_ / num_random);
          reqs.push_back(SingleRead(start_idx, read_size_ / num_random));
        }

        while (!request_queue_->try_enqueue(DBRequest(client_id_, reqs)) &&
               !stop.load()) {
          std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
      }
      cout << "Client ID " << client_id_ << " completed." << endl;
    });
    return client_thread;
  }

private:
  const int client_id_;
  const std::shared_ptr<ReaderWriterQueue<DBRequest>> &request_queue_;
  const size_t db_size_elements_;
  const size_t read_size_;
  const size_t compute_duration_ms_;
};
#endif