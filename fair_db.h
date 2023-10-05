#ifndef FAIR_DB_H
#define FAIR_DB_H

#include <chrono>
#include <iostream>
#include <numeric>
#include <time.h>
#include <vector>

#include "db_client.h"

using namespace std;
using namespace moodycamel;

// TODO: create github repo, set up auto file transfer

// TODOs:
// bring up DB once, run multiple queries
// create class for interacting with queue (+ metrics)
// take flags as input (no more need for code copying)
// thread pool
// need better logic for handling empty queue?

// TODO: update stats to a histogram
struct RunStats {
  vector<int> read_counts;
  int total_reads = 0;

  vector<vector<int>> query_durations;
  long dummy = 0;

  RunStats(size_t num_clients, size_t num_reads) {
    read_counts = std::vector<int>(num_clients, 0);
    query_durations.resize(num_clients);
    for (int i = 0; i < num_clients; ++i) {
      query_durations[i].resize(num_reads / num_clients);
    }
  }
};

class FairDB {
public:
  FairDB(size_t db_size_elements,
         vector<ReaderWriterQueue<DBRequest> *> client_queues)
      : db_size_elements_(db_size_elements), client_queues_(client_queues) {}
  void Init() {
    cout << "Initializing db of size: "
         << db_size_elements_ * datatype_size_ / 1e9 << "GB" << endl;
    db_.resize(db_size_elements_);
    for (int i = 0; i < db_.size(); ++i) {
      db_[i] = i;
    }
    cout << "Initialization complete." << endl;
  }

  RunStats Run(size_t num_reads) {
    auto stats = RunStats(client_queues_.size(), num_reads);

    size_t num_clients = client_queues_.size();

    int queue_idx = -1;
    DBRequest req(-1, {});
    while (stats.total_reads < num_reads) {
      queue_idx = (queue_idx + 1) % num_clients;
      while (!client_queues_[queue_idx]->try_dequeue(req)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }

      auto start = std::chrono::high_resolution_clock::now();
      for (const auto read : req.reads) {
        auto ministart = std::chrono::high_resolution_clock::now();
        // cout << "read size: " << read.read_size << endl;
        // cout << "start: " << read.start_idx << endl;
        // cout << "end: " << read.start_idx + read.read_size << endl;
        for (int j = 0; j < read.read_size; ++j) {
          stats.dummy += db_[read.start_idx + j] % 2;
        }
        auto ministop = std::chrono::high_resolution_clock::now();
        auto miniduration =
            std::chrono::duration_cast<std::chrono::microseconds>(ministop -
                                                                  ministart);
        // cout << "dur: " << miniduration.count() << endl;;
      }
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
      stats.query_durations[queue_idx][stats.read_counts[queue_idx]] =
          duration.count();
      ++stats.read_counts[queue_idx];
      ++stats.total_reads;
    }
    return stats;
  }

private:
  size_t db_size_elements_;
  size_t datatype_size_ = sizeof(int);
  // TODO: pointer wrapper
  vector<int> db_;
  vector<ReaderWriterQueue<DBRequest> *> client_queues_;
};

#endif