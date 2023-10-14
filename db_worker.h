#ifndef DB_WORKER_H
#define DB_WORKER_H

#include <algorithm>
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

// TODO: clean constructor up
struct QueueState {
  vector<ClientQueue> client_queues;
  vector<int> cur_cpu;
  vector<int> cur_disk_bw;
  int cur_queue_idx = -1;

  QueueState() : cur_cpu(2, 0), cur_disk_bw(2, 0){};
  QueueState(vector<ClientQueue> client_queues)
      : client_queues(client_queues), cur_cpu(2, 0), cur_disk_bw(2, 0) {}
};

// TODO: histogram, nicer output format
struct QueryStats {
  int queue_idx;
  long start;
  long duration;
};

struct RunStats {
  vector<int> read_counts;
  int total_reads = 0;
  long dummy = 0;
  vector<QueryStats> query_stats;

  RunStats(size_t num_clients, size_t num_reads) {
    read_counts = std::vector<int>(num_clients, 0);
    query_stats.resize(num_reads);
  }
};

class DBWorker {
public:
  DBWorker(const shared_ptr<FairDB> db, std::shared_ptr<QueueState> queue_state,
           std::shared_ptr<std::mutex> queue_mutex)
      : db_(db), queue_state_(queue_state), queue_mutex_(queue_mutex) {}

  void Run(int worker_id, size_t num_queries) {
    size_t num_clients = queue_state_->client_queues.size();
    auto stats = RunStats(num_clients, num_queries);

    DBRequest req(-1, {});
    while (stats.total_reads < num_queries) {
      // TODO: error handling
      int queue_idx = PullRequestFromQueues(req);

      auto start = std::chrono::high_resolution_clock::now();
      for (const auto read : req.reads) {
        // cout << "read size: " << read.read_size << endl;
        // cout << "start: " << read.start_idx << endl;
        // cout << "end: " << read.start_idx + read.read_size << endl;
        db_->Read(stats.dummy, read.start_idx, read.read_size);
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
      }
      auto stop = std::chrono::high_resolution_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
      // cout << "dur: " << duration.count() << endl;;

      auto time_since_epoch_ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              start.time_since_epoch())
              .count();

      stats.query_stats[stats.total_reads] =
          QueryStats{.queue_idx = queue_idx,
                     .start = time_since_epoch_ms,
                     .duration = duration.count()};

      ++stats.read_counts[queue_idx];
      ++stats.total_reads;
      PushResultsToQueues(queue_idx, duration.count());
    }
    DumpStats(stats, worker_id, num_clients);
  }

private:
  const shared_ptr<FairDB> db_;

  std::shared_ptr<QueueState> queue_state_;
  std::shared_ptr<std::mutex> queue_mutex_;

  // TODO: improve multi-threading. Currently multiple threads will choose
  // the same least-service queue
  int MinimumServiceScheduling() {
    int min_service = std::numeric_limits<int>::max();
    int min_idx;
    for (int i = 0; i < queue_state_->client_queues.size(); ++i) {
      const auto &q = queue_state_->client_queues[i];
      if (q.service_us < min_service) {
        min_service = q.service_us;
        min_idx = i;
      }
    }
    return min_idx;
  }

  int DRFScheduling() {
    const float max_cpu = 18.0;
    const float max_disk_bw = 9.0;

    float client1_cpu_ratio = queue_state_->cur_cpu[0] / max_cpu;
    float client1_disk_ratio = queue_state_->cur_disk_bw[0] / max_disk_bw;
    float client1_max_ratio = std::max(client1_cpu_ratio, client1_disk_ratio);

    float client2_cpu_ratio = queue_state_->cur_cpu[1] / max_cpu;
    float client2_disk_ratio = queue_state_->cur_disk_bw[1] / max_disk_bw;
    float client2_max_ratio = std::max(client2_cpu_ratio, client2_disk_ratio);

    int idx = client2_max_ratio < client1_max_ratio;

    // cout << "Before: " << idx << ": " << queue_state_->cur_disk_bw[0] << ", "
    // << queue_state_->cur_cpu[0] << ", " << queue_state_->cur_disk_bw[1] << ",
    // " << queue_state_->cur_cpu[1] << endl;
    if (idx == 0) {
      // Task A: 1/3GB read, 2/3s CPU
      queue_state_->cur_disk_bw[0] += 1; // out of 9
      queue_state_->cur_cpu[0] += 4;     // out of 18
    } else {
      // Task B: 1GB read, 1/6s CPU
      queue_state_->cur_disk_bw[1] += 3; // out of 9
      queue_state_->cur_cpu[1] += 1;     // out of 18
    }
    // cout << "After: " << idx << ": " << queue_state_->cur_disk_bw[0] << ", "
    // << queue_state_->cur_cpu[0] << ", " << queue_state_->cur_disk_bw[1] << ",
    // " << queue_state_->cur_cpu[1] << endl;
    return idx;
  }

  int RoundRobinScheduling() {
    return (queue_state_->cur_queue_idx + 1) %
           queue_state_->client_queues.size();
  }

  // TODO: DWRR - give each queue some quantum each round.
  // int DeficitWeightedRoundRobin(DBRequest &request) {}

  int PullRequestFromQueues(DBRequest &request) {
    std::lock_guard<std::mutex> lock(*queue_mutex_);

    // queue_state_->cur_queue_idx = MinimumServiceScheduling();
    queue_state_->cur_queue_idx = RoundRobinScheduling();
    // queue_state_->cur_queue_idx = DRFScheduling();

    while (!queue_state_->client_queues[queue_state_->cur_queue_idx]
                .queue->try_dequeue(request)) {
      // std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
    return queue_state_->cur_queue_idx;
  }

  void PushResultsToQueues(int queue_idx, int service_duration_us) {
    std::lock_guard<std::mutex> lock(*queue_mutex_);
    queue_state_->client_queues[queue_idx].service_us += service_duration_us;

    if (queue_idx == 0) {
      // Task A: 1/3GB read, 2/3s CPU
      queue_state_->cur_disk_bw[0] -= 1; // out of 9
      queue_state_->cur_cpu[0] -= 4;     // out of 18
    } else {
      // Task B: 1GB read, 1/6s CPU
      queue_state_->cur_disk_bw[1] -= 3; // out of 9
      queue_state_->cur_cpu[1] -= 1;     // out of 18
    }
  }

  void DumpStats(RunStats &stats, int worker_id, int num_clients) {
    vector<vector<int>> per_client_durations;
    per_client_durations.resize(num_clients);
    for (const auto query : stats.query_stats) {
      per_client_durations[query.queue_idx].push_back(query.duration);
    }

    vector<vector<int>> per_client_starts;
    per_client_starts.resize(num_clients);
    for (const auto query : stats.query_stats) {
      per_client_starts[query.queue_idx].push_back(query.start);
    }

    for (int i = 0; i < per_client_durations.size(); ++i) {
      const auto durs = per_client_durations[i];
      int duration_avg =
          std::accumulate(durs.begin(), durs.end(), 0) / durs.size();
      cout << "Avg: " << i << " - " << duration_avg << " (dummy=" << stats.dummy
           << ")" << endl;
    }

    std::ofstream output_file("results/results_" + std::to_string(worker_id) +
                              ".txt");
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

    for (const auto starts : per_client_starts) {
      for (const auto s : starts) {
        output_file << s << ", ";
      }
      output_file << endl << endl;
    }
    output_file << endl;
    output_file.close();
  }
};

#endif