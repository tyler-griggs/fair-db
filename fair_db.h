#ifndef FAIR_DB_H
#define FAIR_DB_H

#include <iostream>
#include <vector>

#include "db_worker.h"
#include "utils.h"

using namespace std;

class FairDB {
public:
  FairDB(size_t db_size_elements) : db_size_elements_(db_size_elements) {}

  void Init() {
    cout << "Initializing db of size: "
         << db_size_elements_ * datatype_size_ / 1e9 << "GB" << endl;
    db_.resize(db_size_elements_);
    for (int i = 0; i < db_.size(); ++i) {
      db_[i] = i;
    }
    cout << "Initialization complete." << endl;
  }

  void Run(std::vector<ReaderWriterQueue<DBRequest> *> client_queues,
           size_t num_threads, int worker_reads) {
    std::vector<std::thread> worker_threads;
    std::shared_ptr<std::mutex> queue_mutex = std::make_shared<std::mutex>();
    const vector<int> *db = database();
    for (int i = 0; i < num_threads; ++i) {
      worker_threads.push_back(
          std::thread([db, client_queues, queue_mutex, worker_reads, i] {
            DBWorker(db, client_queues, queue_mutex).Run(i, worker_reads);
          }));
      // TODO: cleaner way to do this (ie, remove constant)
      SetThreadAffinity(worker_threads[i], 2 + i);
    }

    for (int i = 0; i < num_threads; ++i) {
      worker_threads[i].join();
    }
  }

  vector<int> *database() { return &db_; }

  size_t db_size_elements() { return db_size_elements_; }
  size_t datatype_size() { return datatype_size_; }

private:
  size_t db_size_elements_;
  size_t datatype_size_ = sizeof(int);
  // TODO: pointer wrapper
  vector<int> db_;
};

#endif