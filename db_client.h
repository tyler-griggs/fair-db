#ifndef DB_CLIENT_H
#define DB_CLIENT_H

#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include "atomicops.h"
#include "common.h"
#include "readerwriterqueue.h"
#include "utils.h"

using namespace moodycamel;
using namespace std;

struct DBClientOptions {
  // Queue to push compute tasks to.
  std::shared_ptr<ReaderWriterQueue<DBRequest>> to_cpu_queue;
  std::shared_ptr<std::mutex> to_cpu_mutex;

  // Queue to push disk read tasks to.
  std::shared_ptr<ReaderWriterQueue<DBRequest>> to_disk_queue;
  std::shared_ptr<std::mutex> to_disk_mutex;

  // Client listens on this queue to receive query completion notifications.
  std::shared_ptr<ReaderWriterQueue<DBRequest>> completion_queue;

  // Defines the task make-up of a query.
  const vector<TaskType> &task_order;

  // Read size of read tasks (in elements).
  const size_t read_size;

  // Compute duration of compute tasks (in milliseconds).
  const int compute_duration_ms;

  // Milliseconds between subsequent queries.
  int query_interval_ms = 0;

  // Maximum outstanding queries for the client.
  int max_outstanding;
};

class DBClient {
public:
  DBClient(int client_id, const DBClientOptions &options,
           size_t db_size_elements)
      : client_id_(client_id), cpu_queue_(options.to_cpu_queue),
        cpu_mutex_(options.to_cpu_mutex), disk_queue_(options.to_disk_queue),
        disk_mutex_(options.to_disk_mutex),
        completion_queue_(options.completion_queue),
        db_size_elements_(db_size_elements), task_order_(options.task_order),
        read_size_(options.read_size),
        compute_duration_ms_(options.compute_duration_ms),
        query_interval_ms_(options.query_interval_ms),
        max_outstanding_(options.max_outstanding) {}

  std::thread RunSequential(std::atomic<bool> &stop) {
    srand(time(0));
    Logger::log("Client ID ", client_id_, " running Sequential workload of ",
                read_size_ / 1e6, "M elements.");
    std::thread client_thread([this, &stop] {
      int outstanding = 0;
      while (!stop.load()) {
        if (outstanding < max_outstanding_) {
          DBRequest db_request = GetSequentialRequest();
          auto queue =
              task_order_[0] == TaskType::DISK_READ ? disk_queue_ : cpu_queue_;
          auto mutex =
              task_order_[0] == TaskType::DISK_READ ? disk_mutex_ : cpu_mutex_;

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
      Logger::log("Client ID ", client_id_, " completed.");
    });
    return client_thread;
  }

  std::thread RunRandom(std::atomic<bool> &stop, int num_random) {
    srand(time(0));
    Logger::log("Client ID ", client_id_, " running Random workload of ",
                read_size_ / 1e6, "M elements.");
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
      Logger::log("Client ID ", client_id_, " completed.");
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
  const vector<TaskType> &task_order_;
  const size_t read_size_;
  const int compute_duration_ms_;
  const int query_interval_ms_;
  const int max_outstanding_;

  DBRequest GetSequentialRequest() {
    int start_idx = rand() % (db_size_elements_ - read_size_);
    std::vector<SingleTask> req{
        SingleTask(start_idx, read_size_, compute_duration_ms_)};

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