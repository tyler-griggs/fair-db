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

#include "common.h"
#include "db_client.h"
#include "fair_db.h"
#include "schedulers.h"

using namespace std;

// TODO: histogram, nicer output format
struct QueryStats {
  int queue_idx;
  int client_id;
  int64_t start; // microseconds since epoch
  // int64_t read_duration;  // microseconds
  int64_t total_duration; // microseconds
};

struct RunStats {
  vector<int> read_counts;
  int total_reads = 0;
  long dummy = 0;
  // TODO: this is dynamically updated in size
  vector<QueryStats> query_stats;

  RunStats(size_t num_clients) {
    read_counts = std::vector<int>(num_clients, 0);
  }
};

struct DBWorkerOptions {
  std::shared_ptr<AllQueueState> input_queue_state;
  std::shared_ptr<std::mutex> input_queue_mutex;

  std::shared_ptr<ReaderWriterQueue<DBRequest>> to_cpu_queue0;
  std::shared_ptr<ReaderWriterQueue<DBRequest>> to_cpu_queue1;
  std::shared_ptr<std::mutex> to_cpu_mutex;

  std::shared_ptr<ReaderWriterQueue<DBRequest>> to_disk_queue0;
  std::shared_ptr<ReaderWriterQueue<DBRequest>> to_disk_queue1;
  std::shared_ptr<std::mutex> to_disk_mutex;

  std::shared_ptr<ReaderWriterQueue<DBRequest>> to_completion_queue0;
  std::shared_ptr<ReaderWriterQueue<DBRequest>> to_completion_queue1;
  std::shared_ptr<std::mutex> to_completion_mutex;

  SchedulerType scheduler_type;

  // 0 - read
  // 1 - compute
  // 2 - read+compute
  int task;
};

class DBWorker {
public:
  DBWorker(int worker_id, const shared_ptr<FairDB> db, DBWorkerOptions options)
      : worker_id_(worker_id), db_(db), options_(options),
        queue_state_(options.input_queue_state), task_(options.task) {}

  bool Init() {
    scheduler_ = GetScheduler(options_.scheduler_type);
    if (scheduler_ == nullptr) {
      cout << "Invalid or missing scheduler. Exiting.";
      return false;
    }
    cout << "DB worker " << worker_id_ << " using scheduler type: "
         << SchedulerToString(options_.scheduler_type) << endl;
    return true;
  }

  void Run(size_t num_clients, std::atomic<bool> &stop) {
    auto stats = RunStats(num_clients);

    DBRequest req(-1, {});
    while (!stop.load()) {
      int queue_idx = PullRequestFromQueues(req, stop);
      if (queue_idx == -1) {
        break;
      }

      auto start = std::chrono::high_resolution_clock::now();
      const auto read = req.reads[0];
      if (task_ == 0 || task_ == 2) {
        /* ====== READ PHASE ======= */
        // TODO: currently assuming a single read
        // cout << "Executing read phase on worker " << worker_id_ << endl;
        db_->Read(stats.dummy, read.start_idx, read.read_size);
      }
      if (task_ == 1 || task_ == 2) {
        // cout << "Executing compute phase on worker " << worker_id_ << endl;
        /* ====== COMPUTE PHASE ======= */
        if (read.compute_duration > 0) {
          auto compute_start = std::chrono::high_resolution_clock::now();
          auto timeout = std::chrono::milliseconds(read.compute_duration);
          volatile int vol_dummy = 0;
          while (std::chrono::high_resolution_clock::now() - compute_start <
                 timeout) {
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

      auto end = std::chrono::high_resolution_clock::now();
      const int64_t total_duration =
          std::chrono::duration_cast<std::chrono::microseconds>(end - start)
              .count();
      // cout << "dur: " << total_duration.count() << endl;;

      auto time_since_epoch_us =
          std::chrono::duration_cast<std::chrono::microseconds>(
              start.time_since_epoch())
              .count();

      stats.query_stats.push_back(QueryStats{.queue_idx = queue_idx,
                                             .client_id = req.client_id,
                                             .start = time_since_epoch_us,
                                             //  .read_duration = read_duration,
                                             .total_duration = total_duration});
      // ++stats.read_counts[queue_idx];
      ++stats.read_counts[req.client_id];
      ++stats.total_reads;

      // TODO: huge update needed to this to separate queue_idx and client_id
      // cout << "Worker " << worker_id_ << " finished task" << endl;
      PushResultsToInputQueues(queue_idx, total_duration);
      PassToNextQueue(req, stop);
    }
    DumpStats(stats, num_clients);
  }

private:
  int worker_id_;
  const shared_ptr<FairDB> db_;
  const DBWorkerOptions &options_;

  std::shared_ptr<AllQueueState> queue_state_;
  std::unique_ptr<TaskScheduler> scheduler_;
  int task_;

  // TODO: release lock earlier. Need to remove assumption that
  //       there are always backlogged requests.
  int PullRequestFromQueues(DBRequest &request, std::atomic<bool> &stop) {
    // TODO: add this back in with parallel workers
    // std::lock_guard<std::mutex> lock(*options_.input_queue_mutex);

    int idx = scheduler_->GetNextQueueIdx(queue_state_, stop);
    if (idx == -1) {
      return -1;
    }
    queue_state_->cur_queue_idx = idx;
    while (!queue_state_->client_queues[idx].queue->try_dequeue(request) &&
           !stop.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return queue_state_->cur_queue_idx;
  }

  void PushResultsToInputQueues(int queue_idx, int service_duration_us) {
    // std::lock_guard<std::mutex> lock(*options_.input_queue_mutex);
    queue_state_->client_queues[queue_idx].service_us += service_duration_us;
  }

  void PassToNextQueue(DBRequest request, std::atomic<bool> &stop) {
    request.cur_task_idx++;
    std::shared_ptr<ReaderWriterQueue<DBRequest>> queue;
    std::shared_ptr<std::mutex> mutex;
    if (request.cur_task_idx >= request.task_order.size()) {
      queue = request.client_id == 0 ? options_.to_completion_queue0
                                     : options_.to_completion_queue1;
      mutex = options_.to_completion_mutex;
    } else if (request.task_order[request.cur_task_idx] == 0) {
      queue = request.client_id == 0 ? options_.to_disk_queue0
                                     : options_.to_disk_queue1;
      mutex = options_.to_disk_mutex;
    } else {
      queue = request.client_id == 0 ? options_.to_cpu_queue0
                                     : options_.to_cpu_queue1;
      mutex = options_.to_cpu_mutex;
    }
    request.queue_start_time =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();
    std::lock_guard<std::mutex> lock(*mutex);
    while (!queue->try_enqueue(request) && !stop.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  void DumpStats(RunStats &stats, int num_clients) {
    vector<vector<int64_t>> per_client_durations;
    per_client_durations.resize(num_clients);
    for (const auto query : stats.query_stats) {
      // per_client_durations[query.queue_idx].push_back(query.total_duration);
      per_client_durations[query.client_id].push_back(query.total_duration);
    }

    vector<vector<int64_t>> per_client_starts;
    per_client_starts.resize(num_clients);
    for (const auto query : stats.query_stats) {
      // per_client_starts[query.queue_idx].push_back(query.start);
      per_client_starts[query.client_id].push_back(query.start);
    }

    for (int i = 0; i < per_client_durations.size(); ++i) {
      const auto durs = per_client_durations[i];
      int duration_avg =
          std::accumulate(durs.begin(), durs.end(), 0) / durs.size();
      cout << "Avg: " << i << " - " << duration_avg << " (dummy=" << stats.dummy
           << ")" << endl;
    }

    std::ofstream output_file("results/results_" + std::to_string(worker_id_) +
                              ".txt");
    output_file << "ExecutionOrder:";
    for (const auto query : stats.query_stats) {
      // output_file << query.queue_idx << ", ";
      output_file << query.client_id << ", ";
    }
    output_file << endl;
    for (const auto durs : per_client_durations) {
      output_file << "ClientDurations:";
      for (const auto d : durs) {
        output_file << d << ", ";
      }
      output_file << endl;
    }

    for (const auto starts : per_client_starts) {
      output_file << "ClientStarts:";
      for (const auto s : starts) {
        output_file << s << ", ";
      }
      output_file << endl;
    }
    output_file.close();
  }
};

#endif