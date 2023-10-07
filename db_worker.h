#ifndef DB_WORKER_H
#define DB_WORKER_H

#include <chrono>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <time.h>
#include <vector>

#include "db_client.h"
#include "fair_db.h"

using namespace std;
using namespace moodycamel;

// TODO: thread pool
// Start with DB threads locking access to queues
// Profile the performance - is lock contention a problem?
// Then work on lock-free version

// TODOs:
// clean up pointer usage: unique_ptr, const, etc.
// bring up DB once, run multiple queries
// take flags as input
// need better logic for handling empty queue?

struct ClientQueue {
  ReaderWriterQueue<DBRequest> *queue;
  int service_us = 0; // Service in microseconds given to this queue

  ClientQueue(ReaderWriterQueue<DBRequest> *queue) : queue(queue) {}
};

struct QueueState {
  vector<ClientQueue> client_queues;
  int cur_queue_idx = -1;

  QueueState(){};
  QueueState(vector<ClientQueue> client_queues)
      : client_queues(client_queues) {}
};

// TODO: histogram, nicer output format
struct QueryStats {
  int queue_idx;
  long duration;
  // TODO: completion timestamp
};

struct RunStats {
  vector<int> read_counts;
  int total_reads = 0;

  vector<QueryStats> query_stats;

  // vector<vector<int>> query_durations;
  long dummy = 0;

  // vector<int> execution_order;

  RunStats(size_t num_clients, size_t num_reads) {
    read_counts = std::vector<int>(num_clients, 0);
    query_stats.resize(num_reads);
  }
};

// TODO: for multi-worker, need a queue lock
class DBWorker {
public:
  DBWorker(vector<int>* db, vector<ReaderWriterQueue<DBRequest> *> client_queues, std::mutex* queue_mutex) : db_(db), queue_mutex_(queue_mutex) {
    for (const auto q : client_queues) {
      queue_state_.client_queues.push_back(ClientQueue(q));
    }
  }

  // std::thread RunOnNewThread(size_t expected_reads) {
  //   std::thread worker_thread(Run, expected_reads);
  //   return worker_thread;
  // }

  // TODO: remove expected reads. Eventually an actual server.
  void Run(size_t expected_reads) {
    size_t num_clients = queue_state_.client_queues.size();
    auto stats = RunStats(num_clients, expected_reads);

    DBRequest req(-1, {});
    while (stats.total_reads < expected_reads) {
      // TODO: error handling
      int queue_idx = PullRequestFromQueues(req);

      auto start = std::chrono::high_resolution_clock::now();
      for (const auto read : req.reads) {
        auto ministart = std::chrono::high_resolution_clock::now();
        // cout << "read size: " << read.read_size << endl;
        // cout << "start: " << read.start_idx << endl;
        // cout << "end: " << read.start_idx + read.read_size << endl;
        for (int j = 0; j < read.read_size; ++j) {
          stats.dummy += (*db_)[read.start_idx + j] % 2;
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

      stats.query_stats[stats.total_reads] =
          QueryStats{.queue_idx = queue_idx, .duration = duration.count()};

      // stats.query_durations[queue_idx][stats.read_counts[queue_idx]] =
      //     duration.count();
      // stats.execution_order[stats.total_reads] = queue_idx;
      ++stats.read_counts[queue_idx];
      ++stats.total_reads;
      PushResultsToQueues(queue_idx, duration.count());
    }
    DumpStats(stats, num_clients);
  }

private:
  vector<int>* db_;
  QueueState queue_state_;
  std::mutex* queue_mutex_;

  // TODO: improve multi-threading. Currently multiple threads will choose
  // the same least-service queue
  int MinimumServiceScheduling() {
    int min_service = std::numeric_limits<int>::max();
    int min_idx;
    std::lock_guard<std::mutex> lock(*queue_mutex_);
    for (int i = 0; i < queue_state_.client_queues.size(); ++i) {
      const auto &q = queue_state_.client_queues[i];
      if (q.service_us < min_service) {
        min_service = q.service_us;
        min_idx = i;
      }
    }
    return min_idx;
  }

  int RoundRobinScheduling() {
    return (queue_state_.cur_queue_idx + 1) % queue_state_.client_queues.size();
  }

  // TODO: DWRR - give each queue some quantum each round.
  // int DeficitWeightedRoundRobin(DBRequest &request) {}

  int PullRequestFromQueues(DBRequest &request) {
    // TODO: locking for multi-threads.
    std::lock_guard<std::mutex> lock(*queue_mutex_);

    // queue_state_.cur_queue_idx = MinimumServiceScheduling();
    queue_state_.cur_queue_idx = RoundRobinScheduling();

    while (!queue_state_.client_queues[queue_state_.cur_queue_idx]
                .queue->try_dequeue(request)) {
      // std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
    return queue_state_.cur_queue_idx;
  }

  void PushResultsToQueues(int queue_idx, int service_duration_us) {
    queue_state_.client_queues[queue_idx].service_us += service_duration_us;
  }

  void DumpStats(RunStats& stats, int num_clients) {
    vector<vector<int>> per_client_durations;
    per_client_durations.resize(num_clients);
    for (const auto query : stats.query_stats) {
      per_client_durations[query.queue_idx].push_back(query.duration);
    }
    for (int i = 0; i < per_client_durations.size(); ++i) {
      const auto durs = per_client_durations[i];
      int duration_avg =
          std::accumulate(durs.begin(), durs.end(), 0) / durs.size();
      cout << "Avg: " << i << " - " << duration_avg << " (dummy=" << stats.dummy << ")"
           << endl;
    }
  }

};



// class FairDB {
// public:
//   // TODO: remove expected reads. Eventually an actual server.
//   RunStats Run(size_t expected_reads) {
//     size_t num_clients = queue_state_.client_queues.size();
//     auto stats = RunStats(num_clients, expected_reads);

//     DBRequest req(-1, {});
//     while (stats.total_reads < expected_reads) {
//       // TODO: error handling
//       int queue_idx = PullRequestFromQueues(req);

//       auto start = std::chrono::high_resolution_clock::now();
//       for (const auto read : req.reads) {
//         auto ministart = std::chrono::high_resolution_clock::now();
//         // cout << "read size: " << read.read_size << endl;
//         // cout << "start: " << read.start_idx << endl;
//         // cout << "end: " << read.start_idx + read.read_size << endl;
//         for (int j = 0; j < read.read_size; ++j) {
//           stats.dummy += db_[read.start_idx + j] % 2;
//         }
//         auto ministop = std::chrono::high_resolution_clock::now();
//         auto miniduration =
//             std::chrono::duration_cast<std::chrono::microseconds>(ministop -
//                                                                   ministart);
//         // cout << "dur: " << miniduration.count() << endl;;
//       }
//       auto stop = std::chrono::high_resolution_clock::now();
//       auto duration =
//           std::chrono::duration_cast<std::chrono::microseconds>(stop - start);

//       stats.query_stats[stats.total_reads] =
//           QueryStats{.queue_idx = queue_idx, .duration = duration.count()};

//       // stats.query_durations[queue_idx][stats.read_counts[queue_idx]] =
//       //     duration.count();
//       // stats.execution_order[stats.total_reads] = queue_idx;
//       ++stats.read_counts[queue_idx];
//       ++stats.total_reads;
//       PushResultsToQueues(queue_idx, duration.count());
//     }
//     return stats;
//   }

// private:
//   // size_t db_size_elements_;
//   // size_t datatype_size_ = sizeof(int);
//   // TODO: pointer wrapper
//   vector<int> db_;
//   QueueState queue_state_;

//   // TODO: this is where we can implement fair scheduling logic
//   int MinimumServiceScheduling() {
//     int min_service = std::numeric_limits<int>::max();
//     int min_idx;
//     for (int i = 0; i < queue_state_.client_queues.size(); ++i) {
//       const auto &q = queue_state_.client_queues[i];
//       if (q.service_us < min_service) {
//         min_service = q.service_us;
//         min_idx = i;
//       }
//     }
//     return min_idx;
//   }

//   int RoundRobinScheduling() {
//     return (queue_state_.cur_queue_idx + 1) % queue_state_.client_queues.size();
//   }

//   // TODO: DWRR - give each queue some quantum each round.
//   // int DeficitWeightedRoundRobin(DBRequest &request) {}

//   int PullRequestFromQueues(DBRequest &request) {
//     // TODO: locking for multi-threads.

//     // queue_state_.cur_queue_idx = MinimumServiceScheduling();
//     queue_state_.cur_queue_idx = RoundRobinScheduling();

//     while (!queue_state_.client_queues[queue_state_.cur_queue_idx]
//                 .queue->try_dequeue(request)) {
//       // std::this_thread::sleep_for(std::chrono::microseconds(10));
//     }
//     return queue_state_.cur_queue_idx;
//   }

//   void PushResultsToQueues(int queue_idx, int service_duration_us) {
//     queue_state_.client_queues[queue_idx].service_us += service_duration_us;
//   }
// };

#endif