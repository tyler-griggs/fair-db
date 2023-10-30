#ifndef DB_CLIENT_H
#define DB_CLIENT_H

#include <chrono>
#include <iostream>
#include <mutex>
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
  int64_t queue_start_time; // microseconds since epoch

  int client_id;
  std::vector<SingleRead> reads;

  // TODO: clean this up
  // 0 - read, 1 - compute
  vector<int> task_order;
  int cur_task_idx = 0;

  DBRequest(int client_id, std::vector<SingleRead> reads)
      : client_id(client_id), reads(reads) {}
};

struct DBClientOptions {
  std::shared_ptr<ReaderWriterQueue<DBRequest>> to_cpu_queue;
  std::shared_ptr<std::mutex> to_cpu_mutex;

  std::shared_ptr<ReaderWriterQueue<DBRequest>> to_disk_queue;
  std::shared_ptr<std::mutex> to_disk_mutex;

  std::shared_ptr<ReaderWriterQueue<DBRequest>> completion_queue;

  int query_interval_ms = 0;
  int max_outstanding;
};

class DBClient {
public:
  DBClient(int client_id, const DBClientOptions &options,
           size_t db_size_elements, const vector<int> &task_order,
           size_t read_size, size_t compute_duration_ms)
      : client_id_(client_id), cpu_queue_(options.to_cpu_queue),
        cpu_mutex_(options.to_cpu_mutex), disk_queue_(options.to_disk_queue),
        disk_mutex_(options.to_disk_mutex),
        completion_queue_(options.completion_queue),
        db_size_elements_(db_size_elements), task_order_(task_order),
        read_size_(read_size), compute_duration_ms_(compute_duration_ms),
        query_interval_ms_(options.query_interval_ms),
        max_outstanding_(options.max_outstanding) {}

  // Run the sequential read workload.
  std::thread RunSequential(std::atomic<bool> &stop) {
    srand(time(0));
    cout << "Client ID " << client_id_ << " running Sequential workload of "
         << read_size_ / 1e6 << "M elements." << endl;
    std::thread client_thread([this, &stop] {
      int outstanding = 0;
      while (!stop.load()) {
        if (outstanding < max_outstanding_) {
          DBRequest db_request = GetSequentialRequest();
          auto queue = task_order_[0] == 0 ? disk_queue_ : cpu_queue_;
          auto mutex = task_order_[0] == 0 ? disk_mutex_ : cpu_mutex_;

          // Queue up a query.
          mutex->lock();
          while (!queue->try_enqueue(db_request) && !stop.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
          }
          mutex->unlock();
          outstanding++;
          // For periodic workloads.
          if (query_interval_ms_ > 0) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(query_interval_ms_));
          }
        } else {
          // Wait for a completion.
          DBRequest request(-1, {});
          while (!completion_queue_->try_dequeue(request) && !stop.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
          }
          outstanding--;
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
        //   std::vector<SingleRead> reqs;
        //   for (int i = 0; i < num_random; ++i) {
        //     int start_idx =
        //         rand() % (db_size_elements_ - read_size_ / num_random);
        //     reqs.push_back(SingleRead(start_idx, read_size_ / num_random));
        //   }

        //   while (!request_queue_->try_enqueue(DBRequest(client_id_, reqs)) &&
        //          !stop.load()) {
        //     std::this_thread::sleep_for(std::chrono::milliseconds(10));
        //   }
      }
      cout << "Client ID " << client_id_ << " completed." << endl;
    });
    return client_thread;
  }

private:
  const int client_id_;
  const std::shared_ptr<ReaderWriterQueue<DBRequest>> &cpu_queue_;
  std::shared_ptr<std::mutex> cpu_mutex_;
  const std::shared_ptr<ReaderWriterQueue<DBRequest>> &disk_queue_;
  std::shared_ptr<std::mutex> disk_mutex_;
  const std::shared_ptr<ReaderWriterQueue<DBRequest>> &completion_queue_;
  const size_t db_size_elements_;
  const vector<int> &task_order_;
  const size_t read_size_;
  const size_t compute_duration_ms_;
  const int query_interval_ms_;
  const int max_outstanding_;

  DBRequest GetSequentialRequest() {
    int start_idx = rand() % (db_size_elements_ - read_size_);
    std::vector<SingleRead> req{
        SingleRead(start_idx, read_size_, compute_duration_ms_)};

    auto db_request = DBRequest(client_id_, req);
    db_request.task_order = task_order_;
    db_request.queue_start_time =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();
    return db_request;
  }
};
#endif