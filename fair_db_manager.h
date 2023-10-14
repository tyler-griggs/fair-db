#ifndef FAIR_DB_MANAGER_H
#define FAIR_DB_MANAGER_H

#include <iostream>
#include <vector>

#include "db_worker.h"
#include "utils.h"

class FairDBManager {
public:
  FairDBManager(size_t db_size_elements) : db_size_elements_(db_size_elements) {
    // database_ = std::make_shared<InMemoryFairDB>();
    database_ = std::make_shared<DiskFairDB>();
  }

  void Init() { database_->Init(db_size_elements_); }

  void Run(std::vector<ReaderWriterQueue<DBRequest> *> client_queues,
           size_t num_threads, int worker_reads) {
    std::vector<std::thread> worker_threads;
    std::shared_ptr<std::mutex> queue_mutex = std::make_shared<std::mutex>();

    auto queue_state = std::make_shared<QueueState>();
    for (const auto q : client_queues) {
      queue_state->client_queues.push_back(ClientQueue(q));
    }

    for (int i = 0; i < num_threads; ++i) {
      worker_threads.push_back(
          std::thread([this, queue_state, queue_mutex, worker_reads, i] {
            DBWorker(database_, queue_state, queue_mutex).Run(i, worker_reads);
          }));
      // TODO: cleaner way to do this (ie, remove constant)
      SetThreadAffinity(worker_threads[i], 1 + i);
    }

    for (int i = 0; i < num_threads; ++i) {
      worker_threads[i].join();
    }
  }

  size_t db_size_elements() { return db_size_elements_; }

private:
  size_t db_size_elements_;
  std::shared_ptr<FairDB> database_;
};

#endif