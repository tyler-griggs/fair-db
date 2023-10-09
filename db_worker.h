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

class DBWorker {
public:
  DBWorker(const shared_ptr<FairDB> db,
           vector<ReaderWriterQueue<DBRequest> *> client_queues,
           std::shared_ptr<std::mutex> queue_mutex)
      : db_(db), queue_mutex_(queue_mutex) {
    for (const auto q : client_queues) {
      queue_state_.client_queues.push_back(ClientQueue(q));
    }
  }

  void Run(int worker_id, size_t num_queries) {
    size_t num_clients = queue_state_.client_queues.size();
    auto stats = RunStats(num_clients, num_queries);

    DBRequest req(-1, {});
    while (stats.total_reads < num_queries) {
      // TODO: error handling
      int queue_idx = PullRequestFromQueues(req);

      auto start = std::chrono::high_resolution_clock::now();
      for (const auto read : req.reads) {
        auto ministart = std::chrono::high_resolution_clock::now();
        // cout << "read size: " << read.read_size << endl;
        // cout << "start: " << read.start_idx << endl;
        // cout << "end: " << read.start_idx + read.read_size << endl;
        db_->Read(stats.dummy, read.start_idx, read.read_size);
        // for (int j = 0; j < read.read_size; ++j) {
        //   stats.dummy += (*db_)[read.start_idx + j] % 2;
        // }


        if (read.compute_duration > 0) {
          auto compute_start = std::chrono::steady_clock::now();
          auto timeout = std::chrono::milliseconds(read.compute_duration);
          volatile int vol_dummy = 0;
          while (std::chrono::steady_clock::now() - compute_start < timeout) {
            for (int i = 0; i < 500000; ++i) {
              vol_dummy += i;
            }
            for (int i = 0; i < 500000; ++i) {
              vol_dummy -= i;
            }
          }
          stats.dummy += vol_dummy;
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
    DumpStats(stats, worker_id, num_clients);
  }

private:
  const shared_ptr<FairDB> db_;
  // const vector<int> *db_;
  QueueState queue_state_;
  std::shared_ptr<std::mutex> queue_mutex_;

  // TODO: improve multi-threading. Currently multiple threads will choose
  // the same least-service queue
  int MinimumServiceScheduling() {
    int min_service = std::numeric_limits<int>::max();
    int min_idx;
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

    queue_state_.cur_queue_idx = MinimumServiceScheduling();
    // queue_state_.cur_queue_idx = RoundRobinScheduling();

    while (!queue_state_.client_queues[queue_state_.cur_queue_idx]
                .queue->try_dequeue(request)) {
      // std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
    return queue_state_.cur_queue_idx;
  }

  void PushResultsToQueues(int queue_idx, int service_duration_us) {
    queue_state_.client_queues[queue_idx].service_us += service_duration_us;
  }

  void DumpStats(RunStats &stats, int worker_id, int num_clients) {
    vector<vector<int>> per_client_durations;
    per_client_durations.resize(num_clients);
    for (const auto query : stats.query_stats) {
      per_client_durations[query.queue_idx].push_back(query.duration);
    }
    for (int i = 0; i < per_client_durations.size(); ++i) {
      const auto durs = per_client_durations[i];
      int duration_avg =
          std::accumulate(durs.begin(), durs.end(), 0) / durs.size();
      cout << "Avg: " << i << " - " << duration_avg << " (dummy=" << stats.dummy
           << ")" << endl;
    }
  
    std::ofstream output_file("results/results_" + std::to_string(worker_id) + ".txt");
    for (const auto query : stats.query_stats) {
      output_file << query.queue_idx << ", ";
    }
    output_file << endl << endl;
    for (const auto durs : per_client_durations) {
      for (const auto d : durs) {
        output_file << d << ", ";
      }
      output_file << endl << endl;
    }
    output_file << endl;
    output_file.close();
  }
};

#endif