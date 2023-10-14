#ifndef DB_MANAGER_H
#define DB_MANAGER_H

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

  void Run(const std::shared_ptr<AllQueueState> queue_state, size_t num_threads,
           const std::vector<int> &worker_cores, int worker_queries) {
    std::vector<std::thread> worker_threads;
    std::shared_ptr<std::mutex> queue_mutex = std::make_shared<std::mutex>();

    for (int i = 0; i < num_threads; ++i) {
      worker_threads.push_back(std::thread([this, queue_state, queue_mutex,
                                            worker_queries, i] {
        DBWorker(database_, queue_state, queue_mutex).Run(i, worker_queries);
      }));
      SetThreadAffinity(worker_threads[i], worker_cores[i]);
    }

    for (int i = 0; i < num_threads; ++i) {
      worker_threads[i].join();
    }
  }

  size_t db_size_elements() { return db_size_elements_; }

private:
  const size_t db_size_elements_;
  std::shared_ptr<FairDB> database_;
};

#endif