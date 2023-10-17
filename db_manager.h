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

  void Init() { 
    database_->Init(db_size_elements_); 
    stop.store(false);
  }

  std::vector<std::thread> Run(std::vector<DBWorkerOptions> worker_options, size_t num_threads,
           const std::vector<int> &worker_cores, int worker_queries, int num_clients) {
    cout << "Running the database workload..." << endl;
    std::vector<std::thread> worker_threads;

    for (int i = 0; i < num_threads; ++i) {
      worker_threads.push_back(std::thread([this, worker_options,
                                            worker_queries, i, num_clients] {
        DBWorker(/*worker_id=*/i, database_, worker_options[i]).Run(worker_queries, num_clients, stop);
      }));
      SetThreadAffinity(worker_threads[i], worker_cores[i]);
    }

    return worker_threads;

    // for (int i = 0; i < num_threads; ++i) {
    //   worker_threads[i].join();
    // }
  }

  void Stop() {
    stop.store(true);
  }

  size_t db_size_elements() { return db_size_elements_; }

private:
  const size_t db_size_elements_;
  std::shared_ptr<FairDB> database_;
  std::atomic<bool> stop;
};

#endif