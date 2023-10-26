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

  bool Init() {
    stop.store(false);
    return database_->Init(db_size_elements_);
  }

  void Run(std::vector<DBWorkerOptions> worker_options, size_t num_threads,
           const std::vector<int> &worker_cores, int run_duration_s,
           int num_clients) {
    cout << "Running the database workload..." << endl;

    std::vector<std::thread> worker_threads;
    for (int i = 0; i < num_threads; ++i) {
      worker_threads.push_back(
          std::thread([this, worker_options, i, num_clients] {
            DBWorker(/*worker_id=*/i, database_, worker_options[i])
                .Run(num_clients, stop);
          }));
      SetThreadAffinity(worker_threads[i], worker_cores[i]);
    }

    std::this_thread::sleep_for(std::chrono::seconds(run_duration_s));
    cout << "Killing DB" << endl;
    stop.store(true);
    for (int i = 0; i < num_threads; ++i) {
      worker_threads[i].join();
    }
  }

  size_t db_size_elements() { return db_size_elements_; }

private:
  const size_t db_size_elements_;
  std::shared_ptr<FairDB> database_;
  std::atomic<bool> stop;
};

#endif